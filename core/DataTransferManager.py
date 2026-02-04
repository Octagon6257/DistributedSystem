from typing import List, Dict, Any, TYPE_CHECKING
from config.LoggingConfig import get_logger

if TYPE_CHECKING:
    from .DataStore import DataStore
    from .NodeRef import RemoteNode

logger = get_logger("DataTransferManager")


class DataTransferManager:

    def __init__(self, data_store: 'DataStore', node_id: int):
        self.data_store = data_store
        self.node_id = node_id

    async def acquire_keys_from_successor(self, successor_node: 'RemoteNode') -> int:
        try:
            predecessor = await successor_node.get_predecessor()
            start_id = predecessor.id if predecessor else successor_node.id
            end_id = self.node_id
            logger.info(f"Retrieving keys from a range ({start_id % 1000 if start_id is not None else None}, {end_id % 1000 if end_id is not None else None}]")
            keys_to_transfer = await successor_node.get_keys_in_range(start_id, end_id)
            if keys_to_transfer:
                data = await successor_node.transfer_keys(keys_to_transfer)
                self.receive_keys(data)
                logger.info(f"Retrieved {len(data)} keys from successor")
                return len(data)
            return 0
        except Exception as e:
            logger.error(f"Error while retrieving keys from a range: {e}")
            return 0

    def get_keys_in_range_local(self, start: int, end: int) -> List[str]:
         return self.data_store.get_keys_in_range(start, end)

    def transfer_keys_local(self, keys: List[str]) -> Dict[str, Any]:
        return self.data_store.transfer_keys(keys)

    def receive_keys(self, key_value_dict: Dict[str, Any]) -> None:
        self.data_store.receive_keys(key_value_dict)