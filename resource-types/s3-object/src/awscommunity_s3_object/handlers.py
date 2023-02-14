import botocore
import traceback
import logging
from typing import (
    Any,
    Dict,
    List,
    MutableMapping,
    Optional,
    Mapping
)
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

from .models import ResourceHandlerRequest, ResourceModel, Tag

# Use this logger to forward log messages to CloudWatch Logs.
CALLBACK_DELAY_SECONDS = 5
LOG = logging.getLogger(__name__)

TYPE_NAME = "AwsCommunity::S3::Object"
resource = Resource(TYPE_NAME, ResourceModel)
test_entrypoint = resource.test_entrypoint

# Define a context for the callback logic.  The value for the 'status'
# key in the dictionary below is consumed in is_callback() and in
# _callback_helper(), that are invoked from a given handler.
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

    except botocore.exceptions.ClientError as ce:
        return _progress_event_failed(
            handler_error_code=_get_handler_error_code(
                ce.response["Error"]["Code"],
            ),
            error_message=str(ce),
            traceback_content=traceback.format_exc(),
        )
    except Exception as e:
        return _progress_event_failed(
            handler_error_code=HandlerErrorCode.InternalFailure,
            error_message=str(e),
            traceback_content=traceback.format_exc(),
        )
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
    if "Tags" in s3_params:
        response = client.put_object(Bucket = s3_params["Bucket"], Key = s3_params["Key"], Body= s3_params["Body"], 
                                    Tagging = s3_params["Tags"]
        )
    else:
        response = client.put_object(Bucket = s3_params["Bucket"], Key = s3_params["Key"], Body= s3_params["Body"])
    return response != None


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
    if model and model.Tags:
        tags = _build_tag_list(model= model, request=request)
        tag_entry = '&'.join(str(tag) for tag in tags)
        put_object_param["Tags"] = tag_entry

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
    except botocore.exceptions.ClientError as ce:
        return _progress_event_failed(
            handler_error_code=_get_handler_error_code(
                ce.response["Error"]["Code"],
            ),
            error_message=str(ce),
            traceback_content=traceback.format_exc(),
        )
    except Exception as e:
        return _progress_event_failed(
            handler_error_code=HandlerErrorCode.InternalFailure,
            error_message=str(e),
            traceback_content=traceback.format_exc(),
        )
    return _progress_event_callback(
        model = model
    )


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
    LOG.debug(f"Progress status: {progress.status}")

    # Callback context logic.
    if _is_callback(
        callback_context,
    ):
        return _callback_helper(
            session,
            request,
            callback_context,
            model,
            is_delete_handler=True,
        )
    else:
        LOG.debug("No callback context present")

    try:
        if model and model.BucketName:
            model_bucket_name = model.BucketName
        if model and model.ObjectKey:
            model_object_key = model.ObjectKey
        
        client = _get_session_client(
            session,
            "s3",
        )
        # Call the Read handler to look for the resource, and return a
        # NotFound handler error code if the resource is not found.
        rh = read_handler(
            session,
            request,
            callback_context,
        )
        if rh.errorCode:
            if rh.errorCode == HandlerErrorCode.NotFound:
                return _progress_event_failed(
                    handler_error_code=HandlerErrorCode.NotFound,
                    error_message=str(rh.message),
                    traceback_content=None,
                )
        if model:
            client.delete_object(
                Bucket=model_bucket_name,
                Key=model_object_key
            )
        
    except botocore.exceptions.ClientError as ce:
        return _progress_event_failed(
            handler_error_code=_get_handler_error_code(
                ce.response["Error"]["Code"],
            ),
            error_message=str(ce),
            traceback_content=traceback.format_exc(),
        )
    except Exception as e:
        return _progress_event_failed(
            handler_error_code=HandlerErrorCode.InternalFailure,
            error_message=str(e),
            traceback_content=traceback.format_exc(),
        )
    return _progress_event_success(
        is_delete_handler=True
    )


@resource.handler(Action.READ)
def read_handler(
    session: Optional[SessionProxy],
    request: ResourceHandlerRequest,
    callback_context: MutableMapping[str, Any],
) -> ProgressEvent:
    model = request.desiredResourceState
    progress: ProgressEvent = ProgressEvent(
        status=OperationStatus.IN_PROGRESS,
        resourceModel=model,
    )
    LOG.debug(f"Progress status: {progress.status}")
    
    try:
        model_object_key = ""
        model_bucket_name = ""
        if model and model.ObjectKey:
            model_object_key = model.ObjectKey
        if model and model.BucketName:
            model_bucket_name = model.BucketName

        client = _get_session_client(
            session,
            "s3",
        )
        
        # Set object body
        response = client.get_object(
            Bucket=model_bucket_name,
            Key=model_object_key
        )
        object_body = response['Body']
        object_body_contents = object_body.read().decode("utf-8")
        if model:
            model.ObjectContents = object_body_contents
        
        # Set ARN
        model.ObjectArn = f"arn:aws:s3:::{model_bucket_name}/{model_object_key}"

        # Set object tags
        response = client.get_object_tagging(
            Bucket=model_bucket_name,
            Key=model_object_key
        )
        tag_set = response['TagSet']
        model.Tags = _get_model_tags_from_tags(tag_set)
        
    except botocore.exceptions.ClientError as ce:
        return _progress_event_failed(
            handler_error_code=_get_handler_error_code(
                ce.response["Error"]["Code"],
            ),
            error_message=str(ce),
            traceback_content=traceback.format_exc(),
        )
    except botocore.exceptions.GeneralServiceException as gse:
        return _progress_event_failed(
            handler_error_code=_get_handler_error_code(
                ce.response["Error"]["Code"],
            ),
            error_message=str(gse),
            traceback_content=traceback.format_exc(),
        )
    except Exception as e:
        return _progress_event_failed(
            handler_error_code=HandlerErrorCode.InternalFailure,
            error_message=str(e),
            traceback_content=traceback.format_exc(),
        )
    return _progress_event_success(
        model=model
    )


@resource.handler(Action.LIST)
def list_handler(
    session: Optional[SessionProxy],
    request: ResourceHandlerRequest,
    callback_context: MutableMapping[str, Any],
) -> ProgressEvent:
    model = request.desiredResourceState
    models = []
    
    try:
        
        if model and model.BucketName:
            model_bucket_name = model.BucketName
        
        client = _get_session_client(
            session,
            "s3"
        )
        
        response = client.list_objects_v2(
            Bucket=model_bucket_name
        )
        if 'Contents' in response.keys():
            contents = response['Contents']
            models = [ ResourceModel(
                ObjectArn=f'arn:aws:s3:::{model_bucket_name}/{content["Key"]}',
                ObjectKey=content['Key'],
                BucketName=model_bucket_name,
                ObjectContents=None,
                Tags=None
            ) for content in contents ]
        else:
            models = []
    except botocore.exceptions.ClientError as ce:
        return _progress_event_failed(
            handler_error_code=_get_handler_error_code(
                ce.response["Error"]["Code"],
            ),
            error_message=str(ce),
            traceback_content=traceback.format_exc(),
        )
    except Exception as e:
        return _progress_event_failed(
            handler_error_code=HandlerErrorCode.InternalFailure,
            error_message=str(e),
            traceback_content=traceback.format_exc(),
        )
    return _progress_event_success(
        is_list_handler=True,
        models=models
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

    
def _get_handler_error_code(
    api_error_code: str,
) -> HandlerErrorCode:
    """Get a handler error code for a given service API error code."""
    LOG.debug("_get_handler_error_code()")

    # Handler error codes in the User Guide for Extension Development:
    # https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/resource-type-test-contract-errors.html
    #
    # Error codes for the Amazon EC2 API:
    # https://docs.aws.amazon.com/AWSEC2/latest/APIReference/errors-overview.html
    if api_error_code == "NoSuchKey":
        return HandlerErrorCode.NotFound
    elif api_error_code in [
        "InvalidParameter",
        "InvalidParameterCombination",
        "InvalidParameterValue",
        "InvalidTagKey.Malformed",
        "MissingAction",
        "MissingParameter",
        "UnknownParameter",
        "ValidationError",
    ]:
        return HandlerErrorCode.InvalidRequest
    # TODO
    # elif api_error_code in [
    # ]:
    #     return HandlerErrorCode.ServiceLimitExceeded
    elif api_error_code in [
        "RequestLimitExceeded"
    ]:
        return HandlerErrorCode.Throttling
    else:
        return HandlerErrorCode.GeneralServiceException


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


def _get_tags_from_previous_resource_tags(
    previous_resource_tags: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    """Create and return a list of tags from request.previousResourceTags."""
    LOG.debug("_get_tags_from_previous_resource_tags()")

    tags = [
        {
            "Key": previous_resource_tag,
            "Value": previous_resource_tags[previous_resource_tag],
        }
        for previous_resource_tag in previous_resource_tags
    ]
    return tags


def _get_tags_from_model_tags(
    model_tags,
) -> List[str]:
    """Create and return a list of tags from model.Tags."""
    LOG.debug("_get_tags_from_model_tags()")
    tags = []
    for model_tag in model_tags: 
        tags += [model_tag.Key + "=" +model_tag.Value]
    return tags


def _get_model_tags_from_tags(
    tags: List[Dict[str, Any]],
) -> List[Tag]:
    """Create and return a list of model.Tags from tags."""
    LOG.debug("_get_model_tags_from_tags()")

    model_tags = [
        Tag(
            Key=tag.get("Key"),
            Value=tag.get("Value"),
        )
        for tag in tags
    ]
    return model_tags


def _get_tags_from_desired_resource_tags(
    desired_resource_tags: Mapping[str, Any],
) -> List[str]:
    """Create and return a list of tags from request.desiredResourceTags."""
    LOG.debug("_get_tags_from_desired_resource_tags()")

    for desired_resource_tag in desired_resource_tags: 
        tags += [desired_resource_tag + "=" +desired_resource_tags[desired_resource_tag]]
    return tags

def _build_tag_list(
    model: Optional[ResourceModel],
    request: ResourceHandlerRequest,
) -> List[Dict[str, Any]]:
    """Build and return a list of resource tags."""
    LOG.debug("_build_tag_list()")

    tags = []

    # Determine if stack-level tags are present.
    if request.desiredResourceTags:
        desired_resource_tags = _get_tags_from_desired_resource_tags(
            request.desiredResourceTags,
        )
        tags += desired_resource_tags

    # Retrieve tags if specified in the model.
    if model and model.Tags:
        model_tags = _get_tags_from_model_tags(
            model.Tags,
        )
        tags += model_tags

    return tags

