package org.entur.kishar.utils;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Objects;


public class IdProcessingParameters implements Serializable {

    @Getter
    @Setter
    private String datasetId;
    @Getter
    @Setter
    private ObjectType objectType;
    @Getter
    @Setter
    private String inputPrefixToRemove;
    @Getter
    @Setter
    private String inputSuffixToRemove;
    @Getter
    @Setter
    private String outputPrefixToAdd;
    @Getter
    @Setter
    private String outputSuffixToAdd;



    /**
     * Apply transformations defined in this class (prefix/suffix removal and after prefix/suffix add) to the input String
     * @param text
     *      input string on which transformation must be applied
     * @return
     *      the transformed string
     */
    public String applyTransformationToString(String text){
        if (StringUtils.isEmpty(text)){
            return text;
        }

        if (inputPrefixToRemove != null && text.startsWith(inputPrefixToRemove)){
            text = text.substring(inputPrefixToRemove.length());
        }

        if (inputSuffixToRemove != null && text.endsWith(inputSuffixToRemove)){
            text = text.substring(0,text.length() - inputSuffixToRemove.length());
        }

        if (outputPrefixToAdd != null && !text.startsWith(outputPrefixToAdd)){
            text = outputPrefixToAdd + text;
        }

        if (outputSuffixToAdd != null && !text.endsWith(outputSuffixToAdd)){
            text = text + outputSuffixToAdd;
        }
        return text;

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IdProcessingParameters that = (IdProcessingParameters) o;
        return Objects.equals(datasetId, that.datasetId) && objectType == that.objectType && Objects.equals(inputPrefixToRemove, that.inputPrefixToRemove) && Objects.equals(inputSuffixToRemove, that.inputSuffixToRemove) && Objects.equals(outputPrefixToAdd, that.outputPrefixToAdd) && Objects.equals(outputSuffixToAdd, that.outputSuffixToAdd);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasetId, objectType, inputPrefixToRemove, inputSuffixToRemove, outputPrefixToAdd, outputSuffixToAdd);
    }
}
