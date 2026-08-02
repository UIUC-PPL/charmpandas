#ifndef __NODEMANAGER_H__
#define __NODEMANAGER_H__

#include <chrono>
#include <random>
#include <queue>
#include <unistd.h>
#include <climits>
#include <map>
#include <cstdlib>
#include <ctime>

extern uint64_t getRandom(); 

unsigned getRandomSeed(){
    typedef std::chrono::high_resolution_clock myclock;
    myclock::time_point beginning = myclock::now();
    myclock::duration d = myclock::now() - beginning;
    return d.count();
}

//Number of samples per PE, on average, in every round
const int SAMPLE_FACTOR = 2;

//Should be set to 1 in most cases
const int SAMPLE_FACTOR_MULTIPLIER = 1;

//epsilon for local sorting (LS)
#define LS_EPS 2

int maxSampleSize(){
    int numprocs = CkNumPes(), numpes = CkNodeSize(CkMyNode()), lognprocs=1;
    while((1<<lognprocs) <= numprocs) lognprocs++;
    return SAMPLE_FACTOR * SAMPLE_FACTOR_MULTIPLIER * numprocs;
}

int sampleSizePerNode(){
    int numproc = CkNumPes();
    int lognprocs = 1, numpes = CkNodeSize(CkMyNode());
    while((1<<lognprocs) <= numproc) lognprocs++;
    return SAMPLE_FACTOR * SAMPLE_FACTOR_MULTIPLIER * numpes;
}

int sampleSizePerPe(){
    return SAMPLE_FACTOR * SAMPLE_FACTOR_MULTIPLIER;
}

int ls_getStride(){
    uint64_t elemsPerPe = numTotalElems/CkNumPes();
    elemsPerPe += ((elemsPerPe * EPS * 2)/100);
    return std::max(1, (int)(((double)elemsPerPe * LS_EPS)/(CkNumNodes() * 100)));
}

/* For random sampling for final local sorting (ls) */

int ls_getMaxSampleSize(){
    int lognpes = 1, numpes = CkNodeSize(CkMyNode());
    while((1<<lognpes) <= numpes) lognpes++;
    return std::max(CkNumNodes()+2, (3 * lognpes * (numpes-1) * 100)/LS_EPS);
}


int ls_getExpectedSampleSize(){
    int lognpes = 1, numpes = CkNodeSize(CkMyNode());
    while((1<<lognpes) <= numpes) lognpes++;
    return (3 * lognpes * (numpes-1) * 100)/LS_EPS;
}


int ls_getSampleSize(int locElems){
    uint64_t elemsPerNode = numTotalElems/CkNumNodes();
    elemsPerNode += ((elemsPerNode * EPS * 2)/100);
    if(elemsPerNode >= ((uint64_t)ls_getExpectedSampleSize() * (uint64_t)locElems)){
        double prob = ((double)ls_getExpectedSampleSize() * (double)locElems)/(double)elemsPerNode;
        return (drand48() <= prob? 1 : 0);
    }
    return std::max(1, (int)(((uint64_t)ls_getExpectedSampleSize() * (uint64_t)locElems)/elemsPerNode));
}


class wrap_ptr{
    public:
        void *ptr;
        wrap_ptr() {} 
        wrap_ptr(void *_ptr): ptr(_ptr) {}
        void pup(PUP::er &p){
            pup_bytes(&p, this, sizeof(*this));
        }
};

class sampleInfo{
    public:
        int size;
        int *indices;
        void *dest;
        int offset;
        sampleInfo() {}
        sampleInfo(int _size, int *_indices, void *_dest, int _offset): 
            size(_size), indices(_indices), dest(_dest), offset(_offset){}
        void pup(PUP::er &p){
            pup_bytes(&p, this, sizeof(*this));
        }
};



class sendInfo{
    public:
        int ind1, ind2;
        void* base;
        sendInfo() {}
        sendInfo(void *_base, int _ind1, int _ind2) : base(_base), ind1(_ind1), ind2(_ind2) {}
        void pup(PUP::er &p){
            pup_bytes(&p, this, sizeof(*this));
        }
};


template<class key>
class sortItem{
    public:
        key k;
        short int peId;
        sortItem(key _k, short int _peId): k(_k), peId(_peId) {}
        friend bool operator<(const sortItem<key>& a, const sortItem<key>& b){
            return (a.k > b.k); //emulate min heap
        }
};


// A received chunk of keys. Replaces data_msg<key>* on the recvOne path:
// with the RDMA post API the keys are delivered into a plain heap buffer that
// we allocate+post, so we just track (ptr, count, sorted) instead of a message.
template <class key>
struct recvBuf {
    key *data;
    int  num_vals;
    bool sorted;
};


template <class key>
class NodeManager : public CBase_NodeManager<key> {
    private:
        int numnodes;
        int numpes;
        int* pelist;
        int* numElem;
        int sampleSize;
        CProxy_Sorter<key> sorter;
        CProxy_Bucket<key> bucket_arr;
        array_msg<key> *sample;
        tagged_key<key> *nodeSample;
        key minkey, maxkey;
        int samplesRcvd, sampleMsgsRcvd;
        int numSent, numRecvd, numFinished;
        tagged_key<key>* ls_sample; //ls_ :: localsort
        tagged_key<key>* splitters;
        int ls_numTotSamples;
        int numDeposited;
        uint64_t numElemFinal;
        int stride;
        //assemble info
        std::vector<std::vector<sendInfo> > ainfo;
        std::vector<recvBuf<key> > recvdMsgs;
        std::vector<recvBuf<key> > bufMsgs;
        std::map<int, uint32_t*> histLocCounts; //one for each pe
        uint32_t* finalHistCounts;

        Bucket<key>* getLocalBucket(){
            for(int i=0; i<numpes; i++){
                Bucket<key> *obj = bucket_arr[pelist[i]].ckLocal();
                if(obj != NULL) return obj;
            }
            CkAbort("GetLocalBucket, getLocalBucket not found \n");
        }


    public:
        NodeManager(key _minkey, key _maxkey): minkey(_minkey), maxkey(_maxkey){
            //ckout<<"Node Manager created at "<<CkMyNode()<<endl;
            numnodes = CkNumNodes();
            numpes = CkNodeSize(CkMyNode());
            numElem = new int[numpes];
            pelist = new int[numpes];
            splitters = new tagged_key<key>[numpes];
            nodeSample = new tagged_key<key>[sampleSizePerNode() * 10]; //allocate extra, just in case
            ainfo.resize(CkNumPes());
            numRecvd = numFinished = numDeposited = numSent = 0;
            ls_numTotSamples=0;
            numElemFinal=0;
            sampleMsgsRcvd=0;
        }

        void registerLocalChare(int nElem, int pe, CProxy_Bucket<key> _bucket_arr,
                CProxy_Sorter<key> _sorter){
            //ckout<<"registerLocalchare called from PE "<<pe<<" at "<<CkMyNode()<<endl;
            //ckout<<"numnodes :"<<numnodes<<" numpes:"<<numpes<<endl;
            //!This should be a bucket_proxy ID
            bucket_arr = _bucket_arr;
            sorter = _sorter;
            static int currnpes = 0;
            pelist[currnpes] = pe;
            numElem[currnpes++] = nElem;
            if(currnpes == numpes){ //all PE's registered
                sampleSize = sampleSizePerNode();
                //ckout<<"sample size: "<<sampleSize<<endl;
                genSampleIndices();
            }
        }

        //call this function
        void genSampleIndices(){
            int *randIndices = new int[sampleSize];
            int cum = numElem[0];
            for(int i=1; i<numpes; i++)
                cum += numElem[i]; 

            unsigned seed = getRandomSeed();
            seed = CkMyNode();
            //ckout<<"Seed is "<<seed<<endl;

            //std::default_random_engine generator(seed);
            //std::uniform_int_distribution<int> distribution(0,cum-1);

            //distribution(generator);

            for(int i=0; i<sampleSize; i++){
                //randIndices[i] = distribution(generator); 
                randIndices[i] = getRandom()%cum; 
                //ckout<<"randIndices "<<randIndices[i]<<endl;
                //distribution.reset();
            }

            sample= new (sampleSize) array_msg<key>;
            sample->numElem = sampleSize;

            std::sort(randIndices, randIndices + sampleSize);
            int cumIndex = 0, cumFreq = 0;
            samplesRcvd = 0;
            for(int i=0; i<numpes; i++){
                int ub = std::upper_bound(randIndices + cumIndex, 
                        randIndices + sampleSize, cumFreq + numElem[i]) - randIndices;   
                //send  randIndices[cumIndex, ub) - cumFreq to PE pelist[i]
                sampleInfo sI(ub-cumIndex, randIndices+cumIndex, sample->data + cumIndex, cumFreq);
                /*
                array_msg<int> *am = new (ub-cumIndex) array_msg<int>;
                am->numElem = ub-cumIndex;
                for(int j=0; j<am->numElem; j++){
                    am->data[j] = randIndices[cumIndex+j] - cumFreq;
                }
                */
                //ckout<<"Sending randIndices["<<cumIndex<<","<<ub<<") - ";
                //ckout<<cumFreq<<" to PE "<<pelist[i]<<" dest: "<<sI.dest<<endl;
                bucket_arr[pelist[i]].genSample(sI);
                cumIndex = ub;
                cumFreq += numElem[i];
            }
        }

        void collectSamples(sampleInfo sI){
            static int count = 0;

            //memcpy(sample->data + samplesRcvd, s->data, s->numElem * sizeof(key));
            samplesRcvd += sI.size;
            count++;
            //can enter here twice, check on the basis of number of msgs received
            if(count == numpes){
                //ckout<<"Sending ******************* to Sorter ************** size: "<< sample->numElem << "from "<<CkMyNode()<<endl;
                //for(int i=0; i<sample->numElem; i++)
                //  ckout<<"From nodemgr "<<CkMyNode()<<" : "<<sample->data[i]<<endl;
                sorter.recvSample(sample);
                samplesRcvd = 0;
            }
        }


        void assembleSamples(std::vector<tagged_key<key> > proc_sample){
            sampleMsgsRcvd++;
            for(int i=0; i<proc_sample.size(); i++){
                nodeSample[i+samplesRcvd] = proc_sample[i];
            }
            samplesRcvd += proc_sample.size();
            if(sampleMsgsRcvd == numpes){
                //ckout<<"All samples received #"<<samplesRcvd<<", maxSampleSize: "<<sampleSizePerNode()<<" - "<<CkMyNode()<<endl;
                sample= new (samplesRcvd) array_msg<key>;
                sample->numElem = samplesRcvd;
                memcpy(sample->data, nodeSample, samplesRcvd*sizeof(tagged_key<key>));
                sorter.recvSample(sample);
                samplesRcvd = 0;
                sampleMsgsRcvd = 0;
            }
        }
        

        void loadkeys(int dest, sendInfo inf){
            ainfo[dest].push_back(inf);   //This will cause synchronization problems.   
            if(ainfo[dest].size() == numpes){
                numSent++;
                this->thisProxy[CkMyNode()].sendOne(dest);
                if(numSent == CkNumNodes())
                    this->thisProxy[CkMyNode()].releaseBufMsgs();
            }
        }

        void sortCopyMsg(data_msg<key> *dm,  int dest){
            std::priority_queue<sortItem<key> > heap;
            CkAssert(ainfo[dest].size() == numpes);
            int first[numpes]; //first element which is not in heap
            for(int i=0; i<ainfo[dest].size(); i++){
                first[i] = ainfo[dest][i].ind1;
                if(first[i] <  ainfo[dest][i].ind2){//not empty
                    key* bucket_data = (key*)ainfo[dest][i].base;
                    heap.push(sortItem<key>(bucket_data[first[i]], i));
                    first[i]++;
                }
            }
            for(int i=0; i<dm->num_vals; i++){
                sortItem<key> sI = heap.top();
                heap.pop();
                key* bucket_data = (key*)ainfo[dest][sI.peId].base;
                dm->data[i] = bucket_data[first[sI.peId] - 1];
                if(first[sI.peId] <  ainfo[dest][sI.peId].ind2){//not empty
                    heap.push(sortItem<key>(bucket_data[first[sI.peId]], sI.peId));
                    first[sI.peId]++;
                }
            }
        }

        

        void sendOne(int dest){
            //CkPrintf("[%d][%d] sendOne to node: %d \n", CkMyNode(), CkMyPe(), dest);
            int numelem = 0;
            for(int i=0; i<ainfo[dest].size(); i++)
                numelem += ainfo[dest][i].ind2 - ainfo[dest][i].ind1;

            // Gather this node's outgoing keys for 'dest' into one contiguous
            // buffer. The keys for a destination are scattered across the local
            // PEs' buckets, so a gather is still needed; but the network transfer
            // is now a zero-copy RDMA write straight into the receiver's posted
            // buffer (no receiver-side intermediate copy). 'sendbuf' must stay
            // alive until the RDMA completes, so it is freed in sendDone().
            key *sendbuf = new key[numelem];
            int curr = 0;
            for(int i=0; i<ainfo[dest].size(); i++){
                int ind1 = ainfo[dest][i].ind1, ind2 = ainfo[dest][i].ind2;
                key* bucket_data = (key*)ainfo[dest][i].base;
                memcpy(sendbuf + curr, bucket_data + ind1, (ind2-ind1)*sizeof(key));
                curr += ind2 - ind1;
            }

            CkPrintf("[N%d] DBG sendOne -> dest=%d numelem=%d\n", CkMyNode(), dest, numelem); //DBG
            CkCallback doneCb(CkIndex_NodeManager<key>::sendDone(NULL),
                              this->thisProxy[CkMyNode()]);
            this->thisProxy[dest].recvOne(CkMyNode(), numelem,
                                          CkSendBuffer(sendbuf, doneCb));
        }


        // Invoked once the zero-copy send of a sendOne() buffer has completed,
        // at which point it is safe to free the gathered send buffer.
        void sendDone(CkDataMsg *m){
            CkNcpyBuffer *src = (CkNcpyBuffer *)(m->data);
            delete [] (key *)(src->ptr);
            delete m;
        }


        void releaseBufMsgs(){
            for(int i=0; i<bufMsgs.size(); i++)
                processRecv(bufMsgs[i]);
            bufMsgs.clear();
        }


        // POST entry method (generated overload of recvOne). Runs when the
        // metadata for the incoming zero-copy send arrives, *before* the data is
        // transferred. We allocate the landing buffer here and post it so the
        // RDMA write lands directly in it; the regular recvOne() below then runs
        // once the transfer completes, with 'keys' pointing at this same buffer.
        // Tag = srcnode: each node sends us exactly one message, so the source
        // node id is unique among in-flight transfers on this node.
        void recvOne(int srcnode, int num_vals, key *keys, CkNcpyBufferPost *ncpyPost){
            CkPrintf("[N%d] DBG POST recvOne from src=%d num_vals=%d (alloc+post)\n", CkMyNode(), srcnode, num_vals); //DBG
            key *buf = new key[num_vals];
            ncpyPost[0].regMode   = CK_BUFFER_REG;
            ncpyPost[0].deregMode = CK_BUFFER_DEREG;
            // PE-level match/post (NOT the *Node* variants): the canonical
            // nodegroup post-API example (examples/.../entry_method_post_api/reg/
            // nodegroupTest) uses CkMatchBuffer/CkPostBuffer even on a nodegroup;
            // the node-level variants are not the supported path on LCI.
            CkPostBuffer(buf, num_vals, srcnode);
            CkMatchBuffer(ncpyPost, 0, srcnode);
        }


        // Regular entry method (ZC completion): runs after the RDMA transfer into
        // 'keys' completes. This is NOT [exclusive] (see .ci), so it must not touch
        // shared per-node state directly -- forward to recvProcess() which is
        // [exclusive]. 'keys' is the node-local posted buffer, valid node-wide.
        void recvOne(int srcnode, int num_vals, key *keys){
            CkPrintf("[N%d] DBG REG recvOne from src=%d num_vals=%d (forward)\n", CkMyNode(), srcnode, num_vals); //DBG
            this->thisProxy[CkMyNode()].recvProcess(srcnode, num_vals, wrap_ptr((void*)keys));
        }

        // [exclusive] shared-state processing of a received key buffer.
        void recvProcess(int srcnode, int num_vals, wrap_ptr keysptr){
            recvBuf<key> rb;
            rb.data = (key*)keysptr.ptr; rb.num_vals = num_vals; rb.sorted = false;
            // Defer processing until this node has issued all of its own sends.
            if(numSent != CkNumNodes()){
                bufMsgs.push_back(rb);
                return;
            }
            processRecv(rb);
        }


        // Shared processing for a received key buffer (called directly once all
        // sends are out, or replayed from bufMsgs via releaseBufMsgs()).
        void processRecv(recvBuf<key> rb){
            CkPrintf("[N%d] DBG processRecv nv=%d recvdMsgs=%zu\n", CkMyNode(), rb.num_vals, recvdMsgs.size()); //DBG
            if(recvdMsgs.size() == 0){
                Bucket<key> *obj = getLocalBucket();
                obj->setTotalKeys();
                stride = ls_getStride();
                numElemFinal = 0;
                //ckout<<" ["<<CkMyNode()<<"] bucket obj : "<<obj<<"   pelist[0]: "<< pelist[0]<<" "<<numTotalElems<<" ls_getMaxSampleSize: "<<ls_getMaxSampleSize()<<" stride: "<<stride<<endl;
                ls_sample = new tagged_key<key>[ls_getMaxSampleSize()];
            }
            int numsamples = ls_getSampleSize(rb.num_vals);
            numElemFinal += rb.num_vals;
            recvdMsgs.push_back(rb);
            ls_numTotSamples += numsamples;
            if(ls_numTotSamples >= ls_getMaxSampleSize()) {
                CkPrintf("[%d] ls_numTotSamples: %d, ls_MaxSampleSize: %d, stride: %d, numElemFinal: %lu, numsamples: %d, msg_size: %d\n",
                            CkMyNode(), ls_numTotSamples, ls_getMaxSampleSize(), ls_getStride(), numElemFinal, numsamples, rb.num_vals);
                CmiAbort("Sample size exceeds expectations");
            }
            //CkPrintf("[%d, %d] Calling handleOne, num_vals: %d, ind: %d, sampleInd: %d, numsamples: %d\n", CkMyNode(), CkMyPe(), rb.num_vals, recvdMsgs.size()-1, ls_numTotSamples - numsamples, numsamples);
            this->thisProxy[CkMyNode()].handleOne(wrap_ptr(), ls_numTotSamples - numsamples, numsamples, recvdMsgs.size()-1);
            ++numRecvd;
            //if(numRecvd == CkNumNodes())
            //  CkPrintf("[%d] Received all messages \n", CkMyNode());
        }


    void handleOne(wrap_ptr msg, int sampleInd, int numsamples, int msgnum){
        //this->thisProxy[CkMyNode()].finishOne();
        //return;
        recvBuf<key> &dm = recvdMsgs[msgnum];
        //CkPrintf("[%d, %d]handleOne,  numSamples: %d, msgsize: %d, sampleInd: %d, maxSamplesize: %d \n", CkMyNode(), CkMyPe(), numsamples, dm.num_vals, sampleInd, ls_getMaxSampleSize());

        if(!dm.sorted){
            std::sort(dm.data, dm.data + dm.num_vals);
            dm.sorted = true;
        }

        /****** Random Sample *******  ===> use for histogram */
        if(dm.num_vals > 0){
            unsigned seed = sampleInd;
            //ckout<<"Seed is "<<seed<<endl;
            /*
               std::default_random_engine generator(seed);
               std::uniform_int_distribution<int> distribution(0,dm.num_vals-1);
               distribution(generator);
             */
            for(int i=0; i<numsamples; i++){
                //int randIdx = distribution(generator);
                int randIdx = getRandom()%dm.num_vals;
                ls_sample[sampleInd + i] = tagged_key<key>(dm.data[randIdx], msgnum, randIdx);
                //distribution.reset();
            }
            if(sampleInd+numsamples >= ls_getMaxSampleSize())
                CkAbort("Numsamples exceeds expectations\n");
        }
        this->thisProxy[CkMyNode()].finishOne();
    }

    void finishOne(){
        ++numFinished;  
        if(numFinished == CkNumNodes()){ //all messages have been received, processed

            ls_sample[ls_numTotSamples++] = getTaggedMaxKey<key>(maxkey);

            //if(!CkMyNode())
            //    CkPrintf("ls_numTotSamples: %d, ls_getMaxSampleSize(): %d, ls_getExpectedSampleSize(): %d\n", ls_numTotSamples, ls_getMaxSampleSize(), ls_getExpectedSampleSize());

            std::sort(ls_sample, ls_sample + ls_numTotSamples); //sort all sampled keys
            for(int i=0; i<numpes; i++){
                uint32_t* histCounts = new uint32_t[ls_numTotSamples+1]; //32bits should suffice
                std::fill(histCounts, histCounts + ls_numTotSamples + 1, 0);
                histLocCounts[pelist[i]] =  histCounts;
            }
            for(int i=0; i<recvdMsgs.size(); i++){
                this->thisProxy[CkMyNode()].localhist(i);
                //this->thisProxy[CkMyNode()].localhist(recvdMsgs[i]);
            }
        }
    }

    void localhist(int msg_num){
        recvBuf<key>& dm = recvdMsgs[msg_num];
        uint32_t* histCounts = (histLocCounts.find(CkMyPe()))->second;
        //CkPrintf("localhist [%d, %d] histCounts: %p \n", CkMyNode(), CkMyPe(), histCounts);
        //for(int i=0; i<dm.num_vals; i++)
        //  CkPrintf("dm.val[%d]: %llu \n", i, dm.data[i]);
        int cumCount = 0;
        tagged_key<key> comp;
        int prb = -1;
        do{
            prb++;
            comp = ls_sample[prb];
            int cnt = lower_bound_tagged(dm.data + cumCount,
                    dm.data + dm.num_vals, comp, msg_num, dm.data) - dm.data;
            histCounts[prb] += cnt - cumCount;
            cumCount = cnt;
        } while(prb<ls_numTotSamples-1);  //the second condition from Bucket.C is not necessary
        this->thisProxy[CkMyNode()].depositHist();
    }

    //how will this be an entry method
    void depositHist(){
        numDeposited++;
        if(numDeposited == CkNumNodes()){
            int numHists = histLocCounts.size();
            uint32_t *histograms[numHists];
            std::map<int, uint32_t*>::iterator it;
            int i;
            for(i=0,it = histLocCounts.begin(); it != histLocCounts.end(); it++, i++)
                histograms[i] = it->second;
            uint32_t cum = 0;
            for(int i=0; i<ls_numTotSamples; i++){
                for(int j=1; j<numHists; j++)
                    histograms[0][i] += histograms[j][i];
                cum += histograms[0][i];
                //if(!CkMyNode())
                //  CkPrintf("[%d] FinalHist[%llu]: %llu \n",  CkMyNode(), ls_sample[i], cum);
            }
            //CkPrintf("[%d] FinalHist: %llu, sample-size: %d \n",  CkMyNode(),  cum, ls_numTotSamples);
            finalHistCounts = histograms[0];
            //CkPrintf("Deposited [%d] All localsort hists deposited \n", CkMyNode());

            //convert to cumulative counts
            for(int i=1; i<ls_numTotSamples; i++)
                finalHistCounts[i] += finalHistCounts[i-1];

            /* Finalize splitters : brute force*/
            int numpes = CkNodeSize(CkMyNode());
            uint32_t elemPerPe = (uint32_t)(numElemFinal/numpes);
            for(int i=0; i<numpes-1; i++){
                int target = (elemPerPe * (i+1));
                //if(!CkMyNode())
                //    CkPrintf("#%d, target: %ld, elemPerPe: %llu \n", i, target, elemPerPe);
                int bestSpltr = -1;
                int closest = INT_MAX;
                for(int j=1; j<ls_numTotSamples; j++){
                    int dist = abs(target - ((int)finalHistCounts[j]));
                    if(dist < closest){
                        closest = dist;
                        bestSpltr = j;
                    }
                }
                splitters[i] = ls_sample[bestSpltr];
            }
            splitters[numpes-1] = getTaggedMaxKey<key>(maxkey);
            std::sort(pelist, pelist + numpes);
            //if(!CkMyNode()){
            for(int i=0; i<numpes; i++){
                //CkPrintf("[%d] Splitterss #%d: %llu\n", CkMyNode(), i, splitters[i]);
            }
            //}
            //Now distribute data among all local threads
            for(int i=0; i<recvdMsgs.size(); i++){
                //for(int j=0; j<recvdMsgs[i]->num_vals; j++)
                //    CkPrintf("[%d] recvdmsg[%d] #(%d): %llu\n", CkMyNode(), i, j, recvdMsgs[i]->data[j]);
                this->thisProxy[CkMyNode()].sendToBuckets(i);
            }
        }
    }

    void sendToBuckets(int msg_num){
        CkPrintf("[N%d] DBG sendToBuckets msg=%d\n", CkMyNode(), msg_num); //DBG
        recvBuf<key>& dm = recvdMsgs[msg_num];
        tagged_key<key> prev = getTaggedMinKey<key>(minkey);
        for(int i=0; i<numpes; i++){
            tagged_key<key> sep1 = prev;
            tagged_key<key> sep2 = splitters[i];
            //find keys and send
            tagged_key<key> comp;
            comp = sep1;
            int ind1 = lower_bound_tagged(dm.data,
                    dm.data + dm.num_vals, comp, msg_num) - dm.data;
            comp = sep2;
            int ind2 = lower_bound_tagged(dm.data,
                    dm.data + dm.num_vals, comp, msg_num) - dm.data;
            //if(!CkMyNode()){
            //  std::cout<<"Finalsending ["<< CkMyNode() <<"] "<<ind1<<"-"<<ind2<<" to "<<pelist[i]<< " - splitters:["<<sep1<<" , "<<sep2<<"]"<<std::endl;
            //}
            bucket_arr[pelist[i]].recvFinalKeys(i, sendInfo(dm.data, ind1, ind2));
            prev = splitters[i];
        }
    }
};

#endif
