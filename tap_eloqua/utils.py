import singer

LOGGER = singer.get_logger()


def check_stream_access(stream_name, probe_fn, auth_error_types, fallback_accessible=False):
    """
    Probe a stream endpoint and return True if accessible, False on auth error.

    :param stream_name: Used in log messages.
    :param probe_fn: Zero-argument callable that performs the API probe.
    :param auth_error_types: Exception type(s) indicating 401/403 — returns False.
    :param fallback_accessible: If True, non-auth errors (e.g. 400 from minimal
                                probe params) are treated as auth-OK and return True.
                                If False (default), they are re-raised.
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
