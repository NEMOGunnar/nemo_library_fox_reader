from dataclasses import dataclass, asdict


@dataclass
class CoupleAttributesRequest:

    attributeIds: list[str]
    previousElementId: str | None = None
    containingGroupInternalName: str | None = None
    dynamicInfoscapeInternalName: str | None = None
    tenant: str = ""
    projectId: str = "" 


    def __init__(self, attributeIds: list[str], previousElementId: str | None = None, containingGroupInternalName: str | None = None,
                 dynamicInfoscapeInternalName: str | None = None):
        self.attributeIds = attributeIds
        self.previousElementId = previousElementId
        self.containingGroupInternalName = containingGroupInternalName
        self.dynamicInfoscapeInternalName = dynamicInfoscapeInternalName

    def to_dict(self):
        """
        Converts the Request instance to a dictionary.
        Returns:
            dict: A dictionary representation of the Request instance.
        """
        return asdict(self)
