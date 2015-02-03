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
          A BigQueryError instance.
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
            return GoogleCloudStorageError('Error reported by server with missing error fields. ' 'Server returned: %s' % (str(server_error),))

        if reason == 'authError':
            return GoogleCloudStorageAuthorizationError(message)

        # We map the less interesting errors to GoogleCloudStorageServiceError.
        return GoogleCloudStorageServiceError(message, error, error_ls, job_ref=job_ref)


class GoogleCloudStorageAuthorizationError(GoogleCloudStorageError):
    """401 error wrapper"""
    pass


class GoogleCloudStorageServiceError(GoogleCloudStorageError):
    pass