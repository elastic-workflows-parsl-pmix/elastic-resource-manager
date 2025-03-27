class Job:
    job_counter = 1
    def __init__(self, min_nodes, max_nodes, execution_time, command, arrival_time, status, type_elasticity):
        self.id = Job.job_counter
        Job.job_counter += 1
        self.min_nodes = min_nodes
        self.max_nodes = max_nodes
        self.execution_time = execution_time
        self.command = command
        self.arrival_time = arrival_time
        self.status = status
        self.type = type_elasticity

    @staticmethod
    def from_dict(job_dict):
        """Create a Job object from a dictionary."""
        return Job(
            min_nodes=job_dict["min_nodes"],
            max_nodes=job_dict["max_nodes"],
            execution_time=job_dict["walltime"],
            command=job_dict["job_command"],
            arrival_time=job_dict["start_time"],
            status=job_dict.get("status", "pending"),
            type_elasticity=job_dict.get("type")
        )