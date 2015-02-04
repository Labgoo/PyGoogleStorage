import textwrap

__author__ = 'krakover'


class GoogleCloudStorageError(Exception):
    # pylint: disable=R0911
    @staticmethod
    def create(error, server_error, error_ls, job_ref=None):
        """Returns a GoogleCloudStorageError for the JSON error that's embedded in the server error response.

        If error_ls contains any errors other than the given one, those are also included in the
        return message.

        :param error: The primary error to convert.
        :param server_error: The error returned by the server. (Only used in case error is malformed.)
        :param error_ls: Additional errors included in the error message.
        :param job_ref: A job reference if its an error associated with a job.
        :return:
          A GoogleCloudStorageError instance.
        """

        reason = error.get('error', None) or error.get('reason', None)
        if job_ref:
            message = 'Error processing %r: %s' % (job_ref, error.get('message'))
        else:
            message = error.get('message')

        new_errors = [err for err in error_ls if err != error]
        if new_errors:
            message += '\nFailure details:\n'
            message += '\n'.join(textwrap.fill(': '.join(filter(None, [err.get('location', None), err.get('message', '')])), initial_indent=' - ', subsequent_indent='   ') for err in new_errors)

        if not reason or not message:
            return GoogleCloudStorageInterfaceError('Error reported by server with missing error fields. ' 'Server returned: %s' % (str(server_error),))

        if reason == 'authError':
            return GoogleCloudStorageAuthorizationError(message)

        if reason == 'notFound':
            return GoogleCloudStorageNotFoundError(message, error, error_ls, job_ref=job_ref)
        if reason == 'backendError':
            return GoogleCloudStorageAuthorizationError(message, error, error_ls, job_ref=job_ref)
        if reason == 'rateLimitExceeded':
            return GoogleCloudStorageRateLimitExceededError(message, error, error_ls, job_ref=job_ref)
        if reason == 'dailyLimitExceeded':
            return GoogleCloudStorageDailyLimitExceededError(message, error, error_ls, job_ref=job_ref)
        if reason == 'accessDenied':
            return GoogleCloudStorageServiceError(message, error, error_ls, job_ref=job_ref)
        if reason == 'backendError':
            return GoogleCloudStorageBackendError(message, error, error_ls, job_ref=job_ref)
        if reason == 'invalidParameter':
            return GoogleCloudStorageInvalidParameterError(message, error, error_ls, job_ref=job_ref)
        if reason == 'badRequest':
            return GoogleCloudStorageBadRequestError(message, error, error_ls, job_ref=job_ref)
        if reason == 'invalidCredentials':
            return GoogleCloudStorageInvalidCredentialsError(message, error, error_ls, job_ref=job_ref)
        if reason == 'insufficientPermissions':
            return GoogleCloudStorageInsufficientPermissionsError(message, error, error_ls, job_ref=job_ref)
        if reason == 'userRateLimitExceeded':
            return GoogleCloudStorageUuserRateLimitExceededError(message, error, error_ls, job_ref=job_ref)
        if reason == 'quotaExceeded':
            return GoogleCloudStorageQuotaExceededError(message, error, error_ls, job_ref=job_ref)

        # We map the less interesting errors to GoogleCloudStorageServiceError.
        return GoogleCloudStorageServiceError(message, error, error_ls, job_ref=job_ref)


class CloudStorageServiceError(GoogleCloudStorageError):
    """Base class of CloudStorage-specific error responses.

    The BigQuery server received request and returned an error.
    """

    def __init__(self, message, error, error_list, job_ref=None, *args, **kwds):
        """Initializes a CloudStorageServiceError.

        :param message: A user-facing error message.
        :param error: The error dictionary, code may inspect the 'reason' key.
        :param error_list: A list of additional entries, for example a load job may contain multiple errors here for each error encountered during processing.
        :param job_ref: Optional job reference.
        :return:
            A BigQueryError instance.
        """
        super(CloudStorageServiceError, self).__init__(message, *args, **kwds)
        self.error = error
        self.error_list = error_list
        self.job_ref = job_ref

    def __repr__(self):
        return '%s: error=%s, error_list=%s, job_ref=%s' % (self.__class__.__name__, self.error, self.error_list, self.job_ref)


class GoogleCloudStorageAuthorizationError(CloudStorageServiceError):
    """403 error wrapper"""
    pass


class GoogleCloudStorageNotFoundError(CloudStorageServiceError):
    """404 error wrapper"""
    pass


class GoogleCloudStorageServiceError(CloudStorageServiceError):
    pass


class GoogleCloudStorageBackendError(CloudStorageServiceError):
    pass


class GoogleCloudStorageInterfaceError(CloudStorageServiceError):
    pass


class GoogleCloudStorageRateLimitExceededError(CloudStorageServiceError):
    pass


class GoogleCloudStorageDailyLimitExceededError(CloudStorageServiceError):
    pass


class GoogleCloudStorageInvalidParameterError(CloudStorageServiceError):
    pass


class GoogleCloudStorageInvalidCredentialsError(CloudStorageServiceError):
    pass


class GoogleCloudStorageBadRequestError(CloudStorageServiceError):
    pass


class GoogleCloudStorageInsufficientPermissionsError(CloudStorageServiceError):
    pass


class GoogleCloudStorageUuserRateLimitExceededError(CloudStorageServiceError):
    pass


class GoogleCloudStorageQuotaExceededError(CloudStorageServiceError):
    pass
