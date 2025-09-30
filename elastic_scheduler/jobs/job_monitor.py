import json
import logging
import os
import threading
import time
from typing import Set, Optional
from elastic_scheduler.jobs import job_id_manager
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileModifiedEvent

from elastic_scheduler.jobs.job import JobSpec, JobRecord, JobRequest

logger = logging.getLogger(__name__)

class JobFileHandler(FileSystemEventHandler):
    """Handler for job file modification events."""
    
    def __init__(self, job_monitor):
        """
        Initialize the job file handler.
        
        Args:
            job_monitor: The JobMonitor instance to notify on file changes
        """
        self.job_monitor = job_monitor
        self.last_modified_time = 0
        # Debounce mechanism to prevent multiple events for a single file save
        self.debounce_seconds = 0.5
    
    def on_modified(self, event):
        """
        Handle file modification events.
        
        Args:
            event: The file system event
        """
        if not isinstance(event, FileModifiedEvent):
            return
            
        # Only process the specific job file we're monitoring
        if event.src_path != os.path.abspath(self.job_monitor.job_file):
            return
            
        # Debounce mechanism to prevent multiple processing of the same change
        current_time = time.time()
        if current_time - self.last_modified_time < self.debounce_seconds:
            return
            
        self.last_modified_time = current_time
        
        # Process the updated job file
        logger.debug(f"Job file modified: {event.src_path}")
        self.job_monitor.process_job_file()


class JobMonitor:
    """Monitors a JSON file for new jobs and submits them to the scheduler."""
    
    def __init__(self, scheduler, job_file: str, job_requests_file: Optional[str] = None):
        """
        Initialize the job monitor.
        
        Args:
            scheduler: The scheduler to submit jobs to
            job_file: Path to the job file to monitor
        """
        self.scheduler = scheduler
        self.job_file = job_file
        self.evolving_job_requests_file = job_requests_file
        self.known_jobs: Set[str] = set()
        self.start_time = time.time()
        self.evolving_poll_seconds = 2  # Default polling interval
        self.observer: Optional[Observer] = None
        self.lock = threading.Lock()
        self._evolving_thread: Optional[threading.Thread] = None  # NEW

    
    def process_job_file(self) -> None:
        """Process the job file and submit new jobs to the scheduler."""
        if not os.path.exists(self.job_file):
            logger.warning(f"Job file does not exist: {self.job_file}")
            return
            
        try:
            with self.lock, open(self.job_file, "r+") as file:
                try:
                    job_data = json.load(file)
                    if not isinstance(job_data, list):
                        logger.error(f"Invalid job data format in {self.job_file}. Expected a list.")
                        return
                        
                    updated = False

                    for job_dict in job_data:
                        if "id" not in job_dict:
                            # Assign a new ID if not present
                            job_dict["id"] = str(job_id_manager.get_next_id())
                            updated = True
                        job_id = str(job_dict.get("id", ""))
                        
                        # Check if the job is pending and ready to start
                        if (job_dict.get("status") == "pending" and job_id not in self.known_jobs):
                            
                            try:
                                spec = JobSpec.from_dict(job_dict)
                                record = JobRecord.from_spec(spec)
                                self.scheduler.submit_job(record)
                                self.known_jobs.add(job_id)
                                
                                logger.info(f"Job submitted: {job_id}")
                                
                                # Update status to "queued" to prevent duplicate submission
                                job_dict["status"] = "queued"
                                updated = True
                            except Exception as e:
                                logger.error(f"Error submitting job {job_id}: {e}")
                    
                    # Write updated job statuses back to the file
                    if updated:
                        file.seek(0)
                        json.dump(job_data, file, indent=4)
                        file.truncate()
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON format in job file: {e}")
                except Exception as e:
                    logger.error(f"Error processing job file: {e}")
        except Exception as e:
            logger.error(f"Error opening job file: {e}")
    
    def start_monitoring(self) -> None:
        """Start monitoring the job file for changes."""
        try:
            # Create a file system observer and event handler
            self.observer = Observer()
            event_handler = JobFileHandler(self)
            
            # Schedule the observer to watch the directory containing the job file
            watch_dir = os.path.dirname(os.path.abspath(self.job_file))
            self.observer.schedule(event_handler, watch_dir, recursive=False)
            
            # Start the observer thread
            self.observer.start()
            logger.info(f"Started monitoring job file: {self.job_file}")
            
            # Process the job file immediately in case there are already jobs
            self.process_job_file()

            # START evolving requests polling thread even when watchdog works
            if self.evolving_job_requests_file and (self._evolving_thread is None or not self._evolving_thread.is_alive()):
                self._evolving_thread = threading.Thread(
                    target=self.monitor_evolving_job_requests,
                    name="evolving-requests-poller",
                    daemon=True,
                )
                self._evolving_thread.start()
                logger.info(f"Started monitoring evolving job requests: {self.evolving_job_requests_file}")
            
        except Exception as e:
            logger.error(f"Error starting job monitor: {e}")
            # Fall back to polling if file watching fails
            self._start_polling_fallback()

    def monitor_evolving_job_requests(self):
        """Parses incoming JSON requests and adds them to the pending list."""
        """Parse incoming JSON scaling requests and submit them once."""
        if not self.evolving_job_requests_file:
            logger.debug("No evolving_job_requests_file configured; skipping monitor.")
            return

        logger.info(f"Started monitoring evolving job requests: {self.evolving_job_requests_file}")
        while True:
            time.sleep(self.evolving_poll_seconds)  # Check for new jobs every 2 seconds
            if not os.path.exists(self.evolving_job_requests_file):
                continue

            try:
                with self.lock, open(self.evolving_job_requests_file, "r+", encoding="utf-8") as file:
                    try:
                        data = json.load(file)
                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON in evolving job requests file: {e}")
                        continue

                    requests = data.get("job_requests", [])
                    if not isinstance(requests, list):
                        logger.error("Invalid format: 'job_requests' must be a list.")
                        continue

                    updated = False
                    for req in requests:
                        status = str(req.get("status", "")).lower()
                        job_id = str(req.get("job_id", ""))
                        scale = str(req.get("scale", "")).lower()
                        num_nodes = req.get("num_nodes")

                        # Only submit pending, well-formed requests
                        if status != "pending":
                            continue
                        if not job_id or scale not in ("expand", "shrink") or not isinstance(num_nodes, int) or num_nodes <= 0:
                            logger.warning(f"Skipping malformed request: {req}")
                            continue

                        try:
                            job_request = JobRequest.from_dict(req)
                            self.scheduler.submit_evolving_job_requests(job_request)
                            logger.info(
                                f"Evolving request submitted: job_id={job_id}, scale={scale}, num_nodes={num_nodes}, req_id={job_request.id}"
                            )
                            # Mark to prevent duplicate submission
                            req["status"] = "arbitrating"
                            updated = True
                        except Exception as e:
                            logger.error(f"Failed submitting evolving request for job {job_id}: {e}")

                    if updated:
                        file.seek(0)
                        json.dump(data, file, indent=4)
                        file.truncate()
            except Exception as e:
                logger.error(f"Error handling evolving job requests: {e}")
    
    def _start_polling_fallback(self) -> None:
        """Fall back to polling if file watching is not available."""
        logger.warning("Falling back to polling-based job monitoring")
        threading.Thread(target=self._poll_job_file, daemon=True).start()
    
    def _poll_job_file(self) -> None:
        """Poll the job file for changes at regular intervals."""
        while True:
            time.sleep(5)  # Check every 5 seconds as a fallback
            self.process_job_file()
    
    def stop_monitoring(self) -> None:
        """Stop monitoring the job file."""
        if self.observer:
            self.observer.stop()
            self.observer.join()
            logger.info("Stopped job file monitoring")