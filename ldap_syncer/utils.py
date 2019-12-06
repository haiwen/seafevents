def str2unicode(data):
    if isinstance(data, str):        return data.decode('utf-8')
    elif isinstance(data, dict):       return dict(map(str2unicode, data.items()))
    elif isinstance(data, tuple):      return tuple(map(str2unicode, data))
    elif isinstance(data, list):       return list(map(str2unicode, data))
    elif isinstance(data, set):        return set(map(str2unicode, data))
    return data
