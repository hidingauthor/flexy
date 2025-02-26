import asyncio

from FederationConnector import FederationConnector
from EBPlanAlpha3 import EBPlanAlpha3
from OriginalEBPlancp import OriginalEBPlancp

if __name__ == "__main__":

    pfd = 1
    fc = FederationConnector()


    m1=OriginalEBPlancp()
    asyncio.run(m1.sample_execution())

    m = EBPlanAlpha3()
    asyncio.run(m.sample_execution())

    fc.disconnect_federation()

