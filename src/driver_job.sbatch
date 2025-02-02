#!/bin/bash
#SBATCH --job-name={job_name}
#SBATCH --account={account_name}
#SBATCH --partition={partition_name}
#SBATCH --nodes={num_nodes}
#SBATCH --ntasks-per-node={tasks_per_node}
#SBATCH --mem={mem_limit}
#SBATCH --cpus-per-task=1
#SBATCH --time=00:30:00
#SBATCH --output={output_filename}

{base_dir}/src/charmrun +p{num_pes} {base_dir}/src/server.out +balancer MetisLB +LBDebug 3 ++server ++server-port 1234 ++nodelist ./slurmnodelist
