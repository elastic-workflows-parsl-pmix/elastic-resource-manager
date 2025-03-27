import threading
import time
import json
from job import Job
import os
import logging

logger = logging.getLogger(__name__)

class JobMonitor:
    """Monitors a JSON file for new jobs and submits them to the scheduler."""
    
    def __init__(self, scheduler, job_file):
        self.scheduler = scheduler
        self.job_file = job_file
        self.known_jobs = set()

    def monitor_jobs(self):
        """Continuously checks the JSON file for new jobs."""
        start_time = time.time()
        while True:
            time.sleep(2)  # Check for new jobs every 2 seconds
            if os.path.exists(self.job_file):
                with open(self.job_file, "r+") as file:
                    try:
                        job_data = json.load(file)
                        if isinstance(job_data, list):
                            updated = False
                            for job_dict in job_data:
                                current_time = time.time()
                                if job_dict.get("status") == "pending" and int(job_dict.get("start_time")) <= (current_time-start_time): 
                                    job = Job.from_dict(job_dict)
                                    self.scheduler.submit_job(job)
                                    print(f"New job {job.id} submitted.")
                                    logger.info(f"Job Submitted: {job.id}")

                                    # Update status to "queued" to prevent duplicate submission
                                    job_dict["status"] = "queued"
                                    updated = True
                            if updated:
                                file.seek(0)
                                json.dump(job_data, file, indent=4)
                                file.truncate()
                    except json.JSONDecodeError:
                        print("Invalid JSON format in job file.")

    def start_monitoring(self):
        """Start the job monitoring in a separate thread."""
        threading.Thread(target=self.monitor_jobs, daemon=True).start()
