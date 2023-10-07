import logging
import time
from typing import Any, Dict, Optional


class AWSAthenaHook:
    """
    Interact with AWS Athena to run, poll queries and return query results

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

    :param sleep_time: Time (in seconds) to wait between two consecutive calls to check query status on Athena
    :type sleep_time: int
    """

    INTERMEDIATE_STATES = (
        'QUEUED',
        'RUNNING',
    )
    FAILURE_STATES = (
        'FAILED',
        'CANCELLED',
    )
    SUCCESS_STATES = ('SUCCEEDED',)

    def __init__(self, client, sleep_time: int = 30) -> None:
        self.client = client
        self.sleep_time = sleep_time

    def run_query(
            self,
            query: str,
            query_context: Dict[str, str],
            result_configuration: Dict[str, Any],
            client_request_token: Optional[str] = None,
            workgroup: str = 'primary',
    ) -> str:
        """
        Run Presto query on athena with provided config and return submitted query_execution_id

        :param query: Presto query to run
        :type query: str
        :param query_context: Context in which query need to be run
        :type query_context: dict
        :param result_configuration: Dict with path to store results in and config related to encryption
        :type result_configuration: dict
        :param client_request_token: Unique token created by user to avoid multiple executions of same query
        :type client_request_token: str
        :param workgroup: Athena workgroup name, when not specified, will be 'primary'
        :type workgroup: str
        :return: str
        """
        params = {
            'QueryString': query,
            'QueryExecutionContext': query_context,
            'ResultConfiguration': result_configuration,
            'WorkGroup': workgroup,
        }
        if client_request_token:
            params['ClientRequestToken'] = client_request_token
        response = self.client.start_query_execution(**params)
        query_execution_id = response['QueryExecutionId']
        return query_execution_id

    def check_query_status(self, query_execution_id):
        """
        Fetch the status of submitted athena query. Returns None or one of valid query states.

        :param query_execution_id: Id of submitted athena query
        :type query_execution_id: str
        :return: str
        """
        response = self.client.get_query_execution(QueryExecutionId=query_execution_id)
        state = None
        try:
            state = response['QueryExecution']['Status']['State']
        except Exception as ex:  # pylint: disable=broad-except
            logging.error('Exception while getting query state %s', ex)
        finally:
            # The error is being absorbed here and is being handled by the caller.
            # The error is being absorbed to implement retries.
            return state  # pylint: disable=lost-exception

    def poll_query_status(self, query_execution_id: str, max_tries: Optional[int] = None) -> Optional[str]:
        """
        Poll the status of submitted athena query until query state reaches final state.
        Returns one of the final states

        :param query_execution_id: Id of submitted athena query
        :type query_execution_id: str
        :param max_tries: Number of times to poll for query state before function exits
        :type max_tries: int
        :return: str
        """
        try_number = 1
        final_query_state = None  # Query state when query reaches final state or max_tries reached
        while True:
            query_state = self.check_query_status(query_execution_id)
            if query_state is None:
                logging.info('Trial %s: Invalid query state. Retrying again', try_number)
            elif query_state in self.INTERMEDIATE_STATES:
                logging.info(
                    'Trial %s: Query is still in an intermediate state - %s', try_number, query_state
                )
            else:
                logging.info(
                    'Trial %s: Query execution completed. Final state is %s}', try_number, query_state
                )
                final_query_state = query_state
                break
            if max_tries and try_number >= max_tries:  # Break loop if max_tries reached
                final_query_state = query_state
                break
            try_number += 1
            time.sleep(self.sleep_time)
        return final_query_state

    def stop_query(self, query_execution_id: str) -> Dict:
        """
        Cancel the submitted athena query

        :param query_execution_id: Id of submitted athena query
        :type query_execution_id: str
        :return: dict
        """
        return self.client.stop_query_execution(QueryExecutionId=query_execution_id)
