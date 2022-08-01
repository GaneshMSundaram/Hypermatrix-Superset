import { NameLabelPair } from "react-querybuilder";
const getOperators = (field: string): NameLabelPair[] => {
    return [
        { name: "=", label: "Equals To" },
        { name: "!=", label: "Not Equals To" },
        { name: ">", label: "Greater Than" },
        { name: "<", label: "Less Than" },
        { name: "like", label: "Like" },
        { name: "isNull", label: "Is Null" },
        { name: "isNotNull", label: "Is Not Null" }
    ]
}

export default getOperators;
