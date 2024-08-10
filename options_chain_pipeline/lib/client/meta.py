class SchwabClientMeta(type):

    _instances = {}

    def __call__(cls, *args, **kwargs):
        client_id = args[0]
        if client_id in SchwabClientMeta._instances:
            return SchwabClientMeta._instances[client_id]
        client = super().__call__(*args, **kwargs)
        SchwabClientMeta._instances[client_id] = client
        return client
