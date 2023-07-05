from abc import abstractmethod, ABC
from krxdata.common.exceptions import WebResponseError


class Web(ABC):
    @abstractmethod
    def get_response(self):
        pass

    @abstractmethod
    def get_raw_data(self):
        pass

    @abstractmethod
    def get_data(self):
        pass

    def _check_response(self, resp):
        if resp.status_code == 200:
            pass
        else:
            raise WebResponseError(resp.status_code, resp.text)

    def _set_params(self):
        params = {}
        params.update(self._params)

        return params

