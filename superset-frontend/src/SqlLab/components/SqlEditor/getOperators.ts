import { NameLabelPair } from "react-querybuilder";
const getOperators = (field: string): NameLabelPair[] => {
    return [
        { name: "equalsTo", label: "Equals To" },
        { name: "notEqualsTo", label: "Not Equals To" },
        { name: "greaterThan", label: "Greater Than" },
        { name: "lessThan", label: "Less Than" },
        { name: "like", label: "Like" },
        { name: "isNull", label: "Is Null" },
        { name: "isNotNull", label: "Is Not Null" }
    ]
}

export default getOperators;
