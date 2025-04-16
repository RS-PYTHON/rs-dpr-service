from pygeoapi.process.base import BaseProcessor

class S1L0_processor(BaseProcessor):
    pass

class S3L0_processor(BaseProcessor):
    pass

# Register the processor

processors = {"S1L0_processor": S1L0_processor,
             "S3L0_processor": S3L0_processor}
