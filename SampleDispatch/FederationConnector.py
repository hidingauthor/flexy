import pyhdb
import sys


class FederationConnector:

    cursors = dict()
    cons = dict()
    federations = dict()
    dropables = dict()

    def __init__(self):
        self.federations[1] = ""
        self.federations[2] = ""
        self.federations[3] = ""
        self.federations[4] = ""

    #39013
    def create_federation_connection(self):
        # Here we provide the connection information for an RDOS with four Container in SAP HANA Cloud. Please add if needed. please setup the connection.

        self.cons[1] = pyhdb.connect(
            host="",
            port=,
            user="",
            password=""
        )
        self.cons[2] = pyhdb.connect(
            host="",
            port=,
            user="",
            password=""
        )
        self.cons[3] = pyhdb.connect(
            host="",
            port=,
            user="",
            password=""
        )
        self.cons[4] = pyhdb.connect(
            host="",
            port=,
            user="",
            password=""
        )
        self.cons[1].setautocommit(True)
        self.cons[2].setautocommit(True)
        self.cons[3].setautocommit(True)
        self.cons[4].setautocommit(True)
        # print("conn: " + str(self.cons[1].isconnected()) + " " + str(self.cons[2].isconnected()) + " " + str(self.cons[3].isconnected()))
        self.cursors[1] = self.cons[1].cursor()
        self.cursors[2] = self.cons[2].cursor()
        self.cursors[3] = self.cons[3].cursor()
        self.cursors[4] = self.cons[4].cursor()


    def disconnect_federation(self):
        for con_id in self.cons:
            try:
                self.cons.get(con_id).close()
            except pyhdb.exceptions.Error:
                print("Unexpected error:", sys.exc_info()[0], " @FD"+str(con_id))
        print("All connections are closed")

