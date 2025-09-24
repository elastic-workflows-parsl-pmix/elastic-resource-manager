import os
import json
import logging
import threading

logger = logging.getLogger(__name__)

class JobIdManager:
    """Manages job ID generation with persistence."""
    
    def __init__(self, persistence_file="job_counter.json"):
        """
        Initialize the job ID manager.
        
        Args:
            persistence_file: File to store the job counter
        """
        self.persistence_file = persistence_file
        self.lock = threading.Lock()
        self._counter = self._load_counter()
    
    def _load_counter(self) -> int:
        """Load the job counter from disk or start at 1 if not available."""
        try:
            if os.path.exists(self.persistence_file):
                with open(self.persistence_file, 'r') as f:
                    data = json.load(f)
                    return int(data.get("counter", 1))
            return 1
        except Exception as e:
            logger.warning(f"Failed to load job counter: {e}. Starting from 1.")
            return 1
    
    def _save_counter(self) -> None:
        """Save the current counter to disk."""
        try:
            with open(self.persistence_file, 'w') as f:
                json.dump({"counter": self._counter}, f)
        except Exception as e:
            logger.error(f"Failed to save job counter: {e}")
    
    def get_next_id(self) -> int:
        """Get the next available job ID."""
        with self.lock:
            job_id = self._counter
            self._counter += 1
            self._save_counter()
            return job_id
    
    def update_counter_if_higher(self, job_id: int) -> None:
        """Update the counter if the given job ID is higher."""
        with self.lock:
            if job_id >= self._counter:
                self._counter = job_id + 1
                self._save_counter()

_provider = JobIdManager()

def get_next_id() -> int:
    """Return the next unique job ID (monotonic, persisted)."""
    return _provider.get_next_id()

def update_counter_if_higher(job_id: int) -> None:
    """Ensure internal counter stays above a seen job_id."""
    _provider.update_counter_if_higher(job_id)