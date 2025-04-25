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

sleep infinity

