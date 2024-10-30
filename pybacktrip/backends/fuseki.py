import requests
from io import BufferedReader
from typing import Union
from typing import Literal as L

from .fuseki_base import FusekiBaseStrategy


class FusekiStrategy(FusekiBaseStrategy):
    def request(
        self,
        method: L["GET", "POST"],
        cmd: Union[str, BufferedReader] = "",
        prefix: bool = True,
        headers: dict = {},
        plainData: bool = False,
        graph: bool = False,
        json: bool = True,
    ) -> dict:
        """Generic REST method caller for the Triplestore

        Args:
            method (Literal["GET", "POST"]): Method of the request.
            cmd (Union[str, BufferedReader], optional): Command to be executed. Defaults to "".
            prefix (bool, optional): If the prefixes need to be added to the query. Defaults to True.
            headers (dict, optional): Custom headers. Defaults to {}.
            plainData (bool, optional): If data needs a format or is plain. Defaults to False.
            graph (bool, optional): If the endpoint needs to specify the graph. Defaults to False.
            json (bool, optional): If the result is a JSON or a dict containing the result as string. Defaults to True.

        Returns:
            dict: Dict containing the result as JSON or text
        """

        if method not in ["GET", "POST"]:
            print("Method unknown")
            return {}

        ep = (
            self.sparql_endpoint
            if not graph
            else f"{self.sparql_endpoint}?graph={self.graph}"
        )

        if prefix and isinstance(cmd, str):
            cmd = (
                " ".join(
                    f"PREFIX {k}: <{v}>" for k, v in self.namespaces().items() if v
                )
                + " "
                + cmd
            )

        try:
            r: requests.Response = requests.request(
                method=method,
                url=ep,
                headers=headers,
                params=({"query": cmd} if method == "GET" and cmd else None),
                data=(
                    cmd
                    if method == "POST" and plainData
                    else {"update": cmd} if method == "POST" and not plainData else None
                ),
            )
            r.raise_for_status()
            if r.status_code == 200:
                return r.json() if json else {"response": r.text}
            return {}
        except requests.RequestException as e:
            print(e)
            return {}
