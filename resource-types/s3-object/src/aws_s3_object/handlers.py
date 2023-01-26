import logging
from typing import Any, MutableMapping, Optional
from cloudformation_cli_python_lib import (
    Action,
    HandlerErrorCode,
    OperationStatus,
    ProgressEvent,
    Resource,
    SessionProxy,
    exceptions,
    identifier_utils,
)

from typing import (
    Any,
    Dict,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
)

from .models import ResourceHandlerRequest, ResourceModel

# Use this logger to forward log messages to CloudWatch Logs.
CALLBACK_DELAY_SECONDS = 5
LOG = logging.getLogger(__name__)

TYPE_NAME = "AWS::S3::Object"
resource = Resource(TYPE_NAME, ResourceModel)
test_entrypoint = resource.test_entrypoint

CALLBACK_STATUS_IN_PROGRESS = {
    "status": OperationStatus.IN_PROGRESS,
}


@resource.handler(Action.CREATE)
def create_handler(
    session: Optional[SessionProxy],
    request: ResourceHandlerRequest,
    callback_context: MutableMapping[str, Any],
) -> ProgressEvent:
    model = request.desiredResourceState
    progress: ProgressEvent = ProgressEvent(
        status=OperationStatus.IN_PROGRESS,
        resourceModel=model,
    )
    # Check if new invocation 
    if _is_callback(callback_context):
        return _callback_helper(
            session,
            request,
            callback_context,
            model        
        )
    else:
        LOG.debug("No callback context present")


    try:
        client = _get_session_client(session, "s3")
        #Prepare kwargs to pass 
        s3_params = _upload_s3_helper(model, request)
        is_uploaded =  _put_object(s3_params = s3_params, client = client)

        if not is_uploaded:
            raise Exception("File Failed to upload")
        # Setting Status to success will signal to cfn that the operation is complete
        progress.status = OperationStatus.SUCCESS
        model.ObjectArn = f"arn:aws:s3:::{model.BucketName}/{model.ObjectKey}"
    except TypeError as e:
        # exceptions module lets CloudFormation know the type of failure that occurred
        raise exceptions.InternalFailure(f"was not expecting type {e}")
        # this can also be done by returning a failed progress event
        # return ProgressEvent.failed(HandlerErrorCode.InternalFailure, f"was not expecting type {e}")

    return _progress_event_callback(
        model = model
    )


def _progress_event_callback(
    model: Optional[ResourceModel],
) -> ProgressEvent:
    """Return a ProgressEvent indicating a callback should occur next."""
    LOG.debug("_progress_event_callback()")

    return ProgressEvent(
        status=OperationStatus.IN_PROGRESS,
        resourceModel=model,
        callbackContext=CALLBACK_STATUS_IN_PROGRESS,
        callbackDelaySeconds=CALLBACK_DELAY_SECONDS,
    )


def _put_object(
    s3_params: Dict[str,Any],
    client
) -> bool:  
    response = client.put_object(Bucket = s3_params["Bucket"], Key = s3_params["Key"], Body= s3_params["Body"])
    if "VersionId" not in response:
        return False
    return True


def _upload_s3_helper(    
    model: Optional[ResourceModel],
    request: ResourceHandlerRequest,
) -> Dict[str, Any]:
    LOG.debug("_upload_s3_helper()")
    if model and model.BucketName and model.ObjectKey and model.ObjectContents:
        put_object_param = {
            "Bucket": model.BucketName,
            "Key": model.ObjectKey,
            "Body": model.ObjectContents
        }
        return put_object_param

@resource.handler(Action.UPDATE)
def update_handler(
    session: Optional[SessionProxy],
    request: ResourceHandlerRequest,
    callback_context: MutableMapping[str, Any],
) -> ProgressEvent:
    model = request.desiredResourceState

    progress: ProgressEvent = ProgressEvent(
        status=OperationStatus.IN_PROGRESS,
        resourceModel=model,
    )
    # Check if update invocation 
    if not _is_callback(callback_context):
        return _callback_helper(
            session,
            request,
            callback_context,
            model        
        )
    else:
        LOG.debug("No callback context present")

    try:
        client = _get_session_client(session, "s3")
        #Prepare kwargs to pass 
        s3_params = _upload_s3_helper(model, request)
        is_uploaded =  _put_object(s3_params = s3_params, client = client)

        if not is_uploaded:
            raise Exception("File Failed to upload")
        # Setting Status to success will signal to cfn that the operation is complete
        progress.status = OperationStatus.SUCCESS
        model.ObjectArn = f"arn:aws:s3:::{model.BucketName}/{model.ObjectKey}"
    except TypeError as e:
        # exceptions module lets CloudFormation know the type of failure that occurred
        raise exceptions.InternalFailure(f"was not expecting type {e}")
        # this can also be done by returning a failed progress event
        # return ProgressEvent.failed(HandlerErrorCode.InternalFailure, f"was not expecting type {e}")

    return read_handler(session, request, callback_context)


@resource.handler(Action.DELETE)
def delete_handler(
    session: Optional[SessionProxy],
    request: ResourceHandlerRequest,
    callback_context: MutableMapping[str, Any],
) -> ProgressEvent:
    model = request.desiredResourceState
    progress: ProgressEvent = ProgressEvent(
        status=OperationStatus.IN_PROGRESS,
        resourceModel=None,
    )
    # TODO: put code here
    return progress


@resource.handler(Action.READ)
def read_handler(
    session: Optional[SessionProxy],
    request: ResourceHandlerRequest,
    callback_context: MutableMapping[str, Any],
) -> ProgressEvent:
    model = request.desiredResourceState
    # TODO: put code here
    return ProgressEvent(
        status=OperationStatus.SUCCESS,
        resourceModel=model,
    )


@resource.handler(Action.LIST)
def list_handler(
    session: Optional[SessionProxy],
    request: ResourceHandlerRequest,
    callback_context: MutableMapping[str, Any],
) -> ProgressEvent:
    # TODO: put code here
    return ProgressEvent(
        status=OperationStatus.SUCCESS,
        resourceModels=[],
    )

def _is_callback(
    callback_context: MutableMapping[str, Any],
) -> bool:
    """Logic to determine whether or not a handler invocation is new."""
    LOG.debug("_is_callback()")

    # If there is a callback context status set, then assume this is a
    # handler invocation (e.g., Create handler) for a previous request
    # that is still in progress.
    if callback_context.get("status") == CALLBACK_STATUS_IN_PROGRESS["status"]:
        return True
    else:
        return False

def _callback_helper(
    session: Optional[SessionProxy],
    request: ResourceHandlerRequest,
    callback_context: MutableMapping[str, Any],
    model: Optional[ResourceModel],
    is_delete_handler: bool = False,
) -> ProgressEvent:
    """Define a callback logic used for resource stabilization."""
    LOG.debug("_callback_helper()")

    # Call the Read handler to determine status.
    rh = read_handler(
        session,
        request,
        callback_context,
    )
    LOG.debug(f"Callback: Read handler status: {rh.status}")
    # Return success if the Read handler returns success.
    if rh.status == OperationStatus.SUCCESS:
        return _progress_event_success(
            model=model,
        )
    elif rh.errorCode:
        LOG.debug(f"Callback: Read handler error code: {rh.errorCode}")
        if rh.errorCode == HandlerErrorCode.NotFound and is_delete_handler:
            LOG.debug("NotFound error in Delete handler: returning success")
            # Return a success status if the resource is not found
            # (thus, assuming it has been deleted).  The Delete
            # handler's response object must not contain a model:
            # hence, the logic driven by is_delete_handler set to True
            # below will not specify a model for ProgressEvent.
            return _progress_event_success(
                is_delete_handler=True,
            )
        elif rh.errorCode == HandlerErrorCode.NotFound:
            return _progress_event_failed(
                handler_error_code=rh.errorCode,
                error_message=rh.message,
                traceback_content=None,
            )
    # Otherwise, call this handler again by using a callback logic.
    else:
        return _progress_event_callback(
            model=model,
        )


def _progress_event_success(
    model: Optional[ResourceModel] = None,
    models: Any = None,
    is_delete_handler: bool = False,
    is_list_handler: bool = False,
) -> ProgressEvent:
    """Return a ProgressEvent indicating a success."""
    LOG.debug("_progress_event_success()")

    if (
        not model
        and not models
        and not is_delete_handler
        and not is_list_handler
    ):
        raise ValueError(
            "Model, or models, or is_delete_handler, or is_list_handler unset",
        )
    # Otherwise, specify 'is_delete_handler' or 'is_list_handler', not both.
    elif is_delete_handler and is_list_handler:
        raise ValueError(
            "Specify either is_delete_handler or is_list_handler, not both",
        )
    # In the case of the Delete handler, just return the status.
    elif is_delete_handler:
        return ProgressEvent(
            status=OperationStatus.SUCCESS,
        )
    # In the case of the List handler, return the status and 'resourceModels'.
    elif is_list_handler:
        return ProgressEvent(
            status=OperationStatus.SUCCESS,
            resourceModels=models,
        )
    else:
        return ProgressEvent(
            status=OperationStatus.SUCCESS,
            resourceModel=model,
        )


def _progress_event_failed(
    handler_error_code: HandlerErrorCode,
    error_message: str,
    traceback_content: Any = None,
) -> ProgressEvent:
    """Log an error, and return a ProgressEvent indicating a failure."""
    LOG.debug("_progress_event_failed()")

    # Choose a logging level depending on the handler error code.
    log_entry = f"""Error message: {error_message},
    traceback content: {traceback_content}"""

    if handler_error_code == HandlerErrorCode.InternalFailure:
        LOG.critical(log_entry)
    elif handler_error_code == HandlerErrorCode.NotFound:
        LOG.debug(log_entry)
    return ProgressEvent.failed(
        handler_error_code,
        f"Error: {error_message}",
    )

def _get_session_client(
    session: Optional[SessionProxy],
    service_name: str,
) -> Any:
    """Create and return a session client for a given service."""
    LOG.debug("_get_session_client()")

    if isinstance(
        session,
        SessionProxy,
    ):
        client = session.client(
            service_name,
        )
        return client
    return None
