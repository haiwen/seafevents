import uuid

def uuid_bytes_to_str(uuid_bytes):
    return str(uuid.UUID(bytes=uuid_bytes))
