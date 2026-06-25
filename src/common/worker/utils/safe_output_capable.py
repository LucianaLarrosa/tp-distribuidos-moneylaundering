from abc import abstractmethod


class SafeOutputCapable:
    @property
    @abstractmethod
    def _control_output_middleware(self):
        pass

    def shutdown(self):
        super().shutdown()
        self._control_output_middleware.close()
