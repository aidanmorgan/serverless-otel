import math
from .constants import NS_PER_MIN

DEFAULT_SEGMENT_SIZE_MINS = 15

def make_segment_identifier(current_nanos: int, segment_bucket_size_minutes: int = DEFAULT_SEGMENT_SIZE_MINS) -> str:
    whole: int = math.floor(current_nanos / (segment_bucket_size_minutes * NS_PER_MIN))
    return f'segment-{int(whole * (segment_bucket_size_minutes * NS_PER_MIN))}'
