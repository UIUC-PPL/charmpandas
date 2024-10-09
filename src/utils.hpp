enum class Operation : int
{
    Read = 0,
    Fetch = 1,
    AddColumn = 2
};

template<class T>
inline T extract(char* &msg, bool increment=true)
{
    T arg = *(reinterpret_cast<T*>(msg));
    if (increment)
        msg += sizeof(T);
    return arg;
}

inline Operation lookup_operation(int opcode)
{
    return static_cast<Operation>(opcode);
}
