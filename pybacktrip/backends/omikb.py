import json
import os
import requests
from io import BufferedReader
from typing import Union, Literal

import yaml

from .fuseki import FusekiStrategy


class OmikbStrategy(FusekiStrategy):
    def __init__(
        self, base_iri: str, triplestore_url: str, database: str, **kwargs
    ) -> None:
        """Initialise the OMIKB triplestore.

        Args:
            base_iri (str): Base IRI to initiate the triplestore from.
            triplestore_url (str): URL of the OMIKB service.
            database (str): Database of the OMIKB to be used.
            kwargs (object): Additional keyword arguments.
        """
        super().__init__(base_iri, triplestore_url, database, **kwargs)

        with open(os.path.expanduser("~/omikb.yml"), "r") as file:
            config = yaml.safe_load(file)

        self.hub_iri = config["jupyter"]["hub"]
        self.hub_token = config["jupyter"]["token"]

        self.endpoint = {
            "GET": config["services"]["kb"]["end_point"]["query"],
            "POST": config["services"]["kb"]["end_point"]["base"],
        }

        print(f"token= {self.hub_token}")

        self.username = config["jupyter"]["username"]
        print(f"hub user name is {self.username}")
        self.hub_api_header = {
            "Authorization": f"token {self.hub_token}",
        }

        response = requests.get(
            f"{self.hub_iri}/hub/api/users/{self.username}", headers=self.hub_api_header
        )
        if response.status_code != 200:
            raise ConnectionError(
                f"Error connecting to Jupyter Hub/fetching user data Failed with: {response.status_code} - \
                      \nSorry, you are not able to use OMI - Contact Admin"
            )

        user_data = response.json()
        auth_state = user_data.get("auth_state", {})
        self.access_token = auth_state.get("access_token", {})
        print(
            f"Hello {self.username}: Your access token is obtained: (Showing last 10 digits only) "
            f"{self.access_token[-10:]}"
        )

        self.userinfo = user_data["auth_state"]["oauth_user"]

        print(
            "Initialised Knowledge Base and OMI access from the jupyter interface for the user:"
        )
        print(print(json.dumps(self.userinfo, indent=2)))

    def _request(
        self,
        method: Literal["GET", "POST"],
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

        ep = self.endpoint[method]

        if prefix and isinstance(cmd, str):
            cmd = (
                " ".join(
                    f"PREFIX {k}: <{v}>" for k, v in self.namespaces().items() if v
                )
                + " "
                + cmd
            )

        headers["Authorization"] = f"Bearer {self.access_token}"
        headers["Accept"] = "application/json"

        try:
            r: requests.Response = requests.request(
                method="POST",
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
