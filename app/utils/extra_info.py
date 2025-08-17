def build(origin, object_=None, flow_id=None, node_id=None, event_id=None, user_id=None,
          error_number=None) -> dict:
    return dict(
        origin=origin,
        class_name=object_.__class__.__name__ if object_ else None,
        package=object_.__class__.__module__ if object_ else None,
        flow_id=flow_id,
        node_id=node_id,
        event_id=event_id,
        user_id=user_id,
        error_number=error_number
    )


def exact(origin, flow_id=None, node_id=None, event_id=None, class_name=None, package=None,
          user_id=None, error_number=None) -> dict:
    return dict(
        origin=origin,
        class_name=class_name,
        package=package,
        flow_id=flow_id,
        node_id=node_id,
        event_id=event_id,
        user_id=user_id,
        error_number=error_number
    )
