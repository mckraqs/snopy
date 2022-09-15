from typing import Dict, List, Optional


class Warehouse:
    def __init__(self, snowflake_connector):
        self.__snowflake_connector = snowflake_connector

    def use(self, warehouse_name: str, silent: bool = False) -> Optional[Dict]:
        """
        Sets particular warehouse for a session
        @param warehouse_name: name of the warehouse to use
        @param silent: whether to run in silent mode (see `Snowflake.execute()`)
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "USE" f" WAREHOUSE {warehouse_name}"

        return self.__snowflake_connector.execute(statement, n=1, silent=silent)

    def get_current(
        self,
    ) -> str:
        """
        Returns the name of the warehouse in use by the current session
        @return: result dictionary (see: `Snowflake.execute()`)
        """
        statement = "SELECT CURRENT_WAREHOUSE()"

        return self.__snowflake_connector.execute(statement, n=1)["results"][0][0]
