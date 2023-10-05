package tooling

type StringSlice []string

func (subset StringSlice) IsSubset(superset []string) bool {
    // Create a map to store the elements of superset for faster lookup
    elementMap := make(map[string]bool)
    for _, item := range superset {
        elementMap[item] = true
    }
    // Check if all elements of subset are present in superset
    for _, item := range subset {
        if !elementMap[item] {
            return false
        }
    }

    return true
}