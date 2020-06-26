import torch
from torch.utils.data.dataloader import DataLoader, _SingleProcessDataLoaderIter




class ShortCircuitSingleProcessDataLoaderIter(_SingleProcessDataLoaderIter):
    def __init__(self, loader, short_circuit_iters = 3, *args, **kwargs):
        super(ShortCircuitSingleProcessDataLoaderIter, self).__init__(loader = loader, *args, **kwargs)
        self.short_circuit_iters = short_circuit_iters
        self.curr_iters = 0

    def _next_data(self):
        if self.curr_iters == self.short_circuit_iters:
            raise StopIteration
        index = self._next_index()  # may raise StopIteration
        data = self._dataset_fetcher.fetch(index)  # may raise StopIteration
        self.curr_iters += 1
        return data


class ShortCircuitDataLoader(DataLoader):
    def __init__(self, short_circuit_iters = 3, *args, **kwargs):
        super(ShortCircuitDataLoader, self).__init__(*args, **kwargs)
        self.short_circuit_iters = short_circuit_iters

    def __iter__(self):
        return ShortCircuitSingleProcessDataLoaderIter(self, self.short_circuit_iters)