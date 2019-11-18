def bytes2str(data):
    if isinstance(data, bytes):        return data.decode()
    elif isinstance(data, (str, int)): return str(data)
    elif isinstance(data, dict):       return dict(map(bytes2str, data.items()))
    elif isinstance(data, tuple):      return tuple(map(bytes2str, data))
    elif isinstance(data, list):       return list(map(bytes2str, data))
    elif isinstance(data, set):        return set(map(bytes2str, data))
    return data