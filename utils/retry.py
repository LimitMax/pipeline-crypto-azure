import time
from utils.logger import logger

def with_retry(func, max_attempts=3, delay=2, backoff=True, *args, **kwargs):
    """
    Jalankan fungsi dengan retry.
    
    Args:
        func (callable): fungsi yang akan dijalankan.
        max_attempts (int): jumlah percobaan maksimal.
        delay (int/float): waktu tunggu awal antar retry (detik).
        backoff (bool): apakah delay ditambah tiap attempt (exponential backoff).
        *args, **kwargs: argumen untuk fungsi.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"Attempt {attempt}/{max_attempts} failed in {func.__name__}: {e}")
            if attempt == max_attempts:
                raise
            wait = delay * attempt if backoff else delay
            time.sleep(wait)
