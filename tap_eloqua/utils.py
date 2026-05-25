import singer

LOGGER = singer.get_logger()


def check_stream_access(stream_name, probe_fn, auth_error_types, fallback_accessible=False):
    """
    Standard Singer tap stream access checker.

    Calls ``probe_fn()`` (a zero-argument callable that performs a lightweight
    API request) to verify whether the configured credentials can reach the
    stream's endpoint. Streams that raise an auth error are excluded from the
    catalog by returning False.

    :param stream_name: Stream name used in log messages.
    :param probe_fn: Zero-argument callable that performs the API probe request.
    :param auth_error_types: Exception type or tuple of exception types that
                             indicate an authentication / authorization failure
                             (e.g. 401 / 403). These cause the function to
                             return False and log a warning.
    :param fallback_accessible: When True, any exception *other* than
                                ``auth_error_types`` is treated as "endpoint
                                reachable, auth OK" (e.g. a 400 caused by
                                intentionally minimal probe params) and the
                                function returns True. When False (default)
                                unexpected exceptions are re-raised.
    :return: True if the stream is accessible, False if an auth error is raised.
    """
    try:
        probe_fn()
        LOGGER.info("Stream '%s' is accessible.", stream_name)
        return True
    except auth_error_types:
        LOGGER.warning(
            "Stream '%s' is not accessible with the provided credentials. "
            "It will be excluded from the catalog.",
            stream_name,
        )
        return False
    except Exception:  # pylint: disable=broad-except
        if fallback_accessible:
            LOGGER.info("Stream '%s' endpoint reachable (auth OK).", stream_name)
            return True
        raise
