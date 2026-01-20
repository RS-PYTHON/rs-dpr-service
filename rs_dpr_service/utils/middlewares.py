# Copyright 2023-2025 Airbus, CS Group
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Common functions for fastapi middlewares"""
import json
from collections.abc import Callable
from http import HTTPStatus
from typing import TypedDict

from fastapi import Request, status
from fastapi.concurrency import iterate_in_threadpool
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware

from rs_dpr_service.utils.logging import Logging

logger = Logging.default(__name__)


class ErrorResponse(TypedDict):
    """A JSON error response returned by the API.

    The STAC API spec expects that `code` and `description` are both present in
    the payload.

    Attributes:
        code: A code representing the error, semantics are up to implementor.
        description: A description of the error.
    """

    code: str
    description: str


class HandleExceptionsMiddleware(BaseHTTPMiddleware):  # pylint: disable=too-few-public-methods
    """
    Middleware to catch all exceptions and return a JSONResponse instead of raising them.
    This is useful in FastAPI when HttpExceptions are raised within the code but need to be handled gracefully.
    """

    async def dispatch(self, request: Request, call_next: Callable):
        try:
            # Call next middleware
            response = await call_next(request)

            # In case of errors, log the response contents
            if 400 <= response.status_code < 600:

                # Read contents
                body = [chunk async for chunk in response.body_iterator]
                dec_content = b"".join(map(lambda x: x if isinstance(x, bytes) else x.encode(), body)).decode()  # type: ignore
                logger.error(f"{response.status_code}: {json.loads(dec_content)}")

                # Reset the StreamingResponse so it can be used again
                response.body_iterator = iterate_in_threadpool(iter(body))

            # Return the response from the next middleware
            return response

        except Exception as exc:  # pylint: disable=broad-exception-caught

            # Log current stack trace
            logger.exception(exc)

            # Calculate HTTP response status code (int) and ErrorResponse code (str) and description (str)
            if isinstance(exc, StarletteHTTPException):
                status_code = exc.status_code
                description = str(exc.detail)
                # Convert e.g. HTTP_500_INTERNAL_SERVER_ERROR into 'InternalServerError'
                phrase = HTTPStatus(exc.status_code).phrase
                str_code = "".join(word.title() for word in phrase.split())

            else:
                # Use generic 400 or 500 code
                status_code = (
                    status.HTTP_400_BAD_REQUEST
                    if HandleExceptionsMiddleware.is_bad_request(request, exc)
                    else status.HTTP_500_INTERNAL_SERVER_ERROR
                )
                description = str(exc)
                str_code = exc.__class__.__name__

            return JSONResponse(status_code=status_code, content=ErrorResponse(code=str_code, description=description))

    @staticmethod
    def is_bad_request(_request: Request, _e: Exception) -> bool:
        """
        Determines if the request that raised this exception shall be considered as a bad request
        and return a 400 error code.

        This function can be overriden by the caller if needed with:
        HandleExceptionsMiddleware.is_bad_request = my_callable
        """
        return False
