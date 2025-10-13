from enum import Enum, IntEnum
import logging
import re
from typing import Type


class FOXAttributeType(IntEnum):
    Normal = 0
    Header = 1
    Summary = 2
    Expression = 3
    Link = 4
    Classification = 5
    CaseDiscrimination = 6


SUMMARY_FUNCTIONS = {
    # define FUNCTION_COUNT    1
    # define FUNCTION_LIST    2
    # define FUNCTION_SUM    3
    # define FUNCTION_MIN    4
    # define FUNCTION_MAX    5
    # define FUNCTION_AVERAGE   6
    # define FUNCTION_PERCENT   7
    # define FUNCTION_PERCENT_DISPLAYED 8
    # define FUNCTION_PREVIOUS   9
    # define FUNCTION_CELL_WIDTH   10
    # define FUNCTION_NUMBER    11
    # define FUNCTION_NEXT    12
    # define FUNCTION_MEDIAN    13
    # define FUNCTION_STANDARD_DEVIATION 14
    # define FUNCTION_VARIANCE   15
    # define FUNCTION_MODAL    16
    # define FUNCTION_PERCENT_SUM  17
    # define FUNCTION_PERCENT_AVERAGE 18
    # define FUNCTION_ACCUMULATION  19
    # define FUNCTION_PERCENTIL   20
    # define FUNCTION_RANKING   21
    # define FUNCTION_PERCENT_ACCUMULATION 22
    # define FUNCTION_FREQUENCY 23
    1: "Count Distinct",
    2: None,  # List not yet supported
    3: "Sum",
    4: "Minimum",
    5: "Maximum",
    6: "Mean",
    7: "Percent share",
    8: None,  # Percent Displayed not yet supported
    9: None,  # Previous not yet supported
    10: None,  # Cell width not yet supported
    11: None,  # Number not yet supported
    12: None,  # Next not yet supported
    13: "Median",
    14: "Standard Deviation",
    15: "Variance",
    16: None,  # Modal not yet supported
    17: None,  # Percent sum not yet supported
    18: None,  # Percent avg not yet supported
    19: None,  # Accumulation not yet supported
    20: None,  # Percentil not yet supported
    21: None,  # Ranking not yet supported
    22: None,  # Percent Accumulation not yet supported
    23: "Frequency",
}

SUMMARY_FUNCTIONS_ALL = {
    1: "Count Distinct",
    2: "List",
    3: "Sum",
    4: "Minimum",
    5: "Maximum",
    6: "Mean",
    7: "Percent share",
    8: "Percent Displayed",
    9: "Previous",
    10: "Cell width",
    11: "Number",
    12: "Next",
    13: "Median",
    14: "Standard Deviation",
    15: "Variance",
    16: "Modal",
    17: "Percent sum",
    18: "Percent avg",
    19: "Accumulation",
    20: "Percentil",
    21: "Ranking",
    22: "Percent Accumulation",
    23: "Frequency",
}


class AggregationFunction(IntEnum):
    Min = 0
    Max = 1
    Mean = 2
    Median = 3
    Sum = 4
    Count = 5
    CountDistinct = 6
    StandardDeviation = 7
    Variance = 8
    PercentSum = 9
    Frequency = 10
    PercentShare = 12
    Unknown = -1
 
 
def get_aggregation_function(function: str) -> AggregationFunction:

    match function:
        case "Count Distinct":
            return AggregationFunction.CountDistinct
        
        case "Sum":
            return AggregationFunction.Sum
        
        case "Minimum":
            return AggregationFunction.Min
        
        case "Maximum":
            return AggregationFunction.Max
        
        case "Mean":
            return AggregationFunction.Mean
        
        case "Percent share":
            return AggregationFunction.PercentShare
        
        case "Median":
            return AggregationFunction.Median
        
        case "Standard Deviation":
            return AggregationFunction.StandardDeviation
        
        case "Variance":
            return AggregationFunction.Variance
        
        case "Frequency":
            return AggregationFunction.Frequency

    return AggregationFunction.Unknown
