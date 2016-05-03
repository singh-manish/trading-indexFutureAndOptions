/*
 The MIT License (MIT)

 Copyright (c) 2015 Manish Kumar Singh

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in all
 copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 SOFTWARE.
 
 */
package tradingstkfutopt;

/**
 * @author Manish Kumar Singh
 */
public class ManualInterventionSignalObject {

    // Index Values for Object
    // Manual Intervention TimeStamp
    public static final int MI_SIGNALTIMESTAMP_INDEX = 0;
    // Level on which to Apply. S for Strategy level, T for trade level    
    public static final int MI_LEVEL_INDEX = 1;
    // Current Slot Number of Open Position if it is for Trade level. 0 if it is for Strategy Level.
    public static final int MI_SLOTNUMBER_INDEX = 2;
    // Action to take. 101 to 199 for Strategy level Actions. 1 to 99 for trade level actions
    public static final int MI_ACTION_INDEX = 3;
    public static final int MI_ACTIONREASON_INDEX = 4;    
    // New Value of given parameter - whether for strategy level OR for Trade Level. Assign to zero if parameter reassignment not reqd.
    public static final int MI_NEWVALUE_INDEX = 5;

    // Number of elements in signal
    public static final int MI_MAX_ELEMENTS = 6;

    private String[] miObjectStructure;
    /*
     Structure of signal is comma separated values
     Index : Meaning / Possible Values
     0  :  Timestamp of signal in YYYYMMDDHHMMSS format
     1  :  Level at which this signal needs to be applied
     "S" OR "Strategy" - To Apply given signal at strategy level
     "T" OR "Trade" - To Apply given signal at particular Trade /Open position level
     2  :  Slot Number at which Given Signal needs to be applied to
     0 - if given signal is at strategy level
     1 to n - Slot number of Open Position on which given signal needs to be applied to
     - check if position at slot number still exists before updating
     3  :  Action code
     101 to 199 - action code pertains to strategy level
     101  - square off all open positions / trade
     102  - update maximum position size as given value 0 - 10 (0 means no new position)
     103  - update Minimum Z Score to given value (0.5 as minmum, 3.0 as maximum)
     104  - update Maximum Z Score to given value (0.5 as minmum, 3.0 as maximum)
     105  - update Minimum Half life to given value (5 as minmum, 100 as maximum)
     106  - update Maximum Half life to given value (5 as minmum, 100 as maximum)

     1 to 99 - Action code pertains to at trade level
     1  - Square Off the trade / position at given slot number 
     2  - Update trade level stop loss to given value 
     3  - Update trade level take profit to given value
     4  - Stop Monitoring (useful for graceful exit)

     4  :  reason for action        
    
     5  :  Target Value of parameter to be updated. 0 if not applicable (e.g. for square off action)        
    
     */

    ManualInterventionSignalObject(String incomingManualInterventionObject) {

        miObjectStructure = new String[MI_MAX_ELEMENTS];

        if (incomingManualInterventionObject.length() > 0) {
            String[] tempObjectStructure = incomingManualInterventionObject.split(",");
            System.arraycopy(tempObjectStructure, 0, miObjectStructure, 0, tempObjectStructure.length);
        }
    }

    // assemble and return Structure
    public String getCompleteMIObjectString() {
        String returnString = "";

        for (int index = 0; index < (miObjectStructure.length - 1); index++) {
            returnString = returnString + miObjectStructure[index] + ",";
        }

        returnString = returnString + miObjectStructure[miObjectStructure.length - 1];

        return (returnString);
    }

    public void setEntryTimeStamp(String newTimeStamp) {
        miObjectStructure[MI_SIGNALTIMESTAMP_INDEX] = newTimeStamp;
    }

    public void setApplyingLevel(String level) {
        miObjectStructure[MI_LEVEL_INDEX] = level;
    }

    public void setSlotNumber(String newSlotNum) {
        miObjectStructure[MI_SLOTNUMBER_INDEX] = newSlotNum;
    }

    public void setActionCode(String newAction) {
        miObjectStructure[MI_ACTION_INDEX] = newAction;
    }

    public void setActionReason(String newReasonForAction) {
        miObjectStructure[MI_ACTIONREASON_INDEX] = newReasonForAction;
    }
    
    public void setTargetValue(String newValueForAction) {
        miObjectStructure[MI_NEWVALUE_INDEX] = newValueForAction;
    }

    public String getEntryTimeStamp() {
        return (miObjectStructure[MI_SIGNALTIMESTAMP_INDEX]);
    }

    public String getApplyingLevel() {
        String returnValue = "NA";
        if (miObjectStructure[MI_LEVEL_INDEX].equalsIgnoreCase("strategy")) {
            returnValue = "S";
        } else if (miObjectStructure[MI_LEVEL_INDEX].equalsIgnoreCase("s")) {
            returnValue = "S";
        } else if (miObjectStructure[MI_LEVEL_INDEX].equalsIgnoreCase("trade")) {
            returnValue = "T";
        } else if (miObjectStructure[MI_LEVEL_INDEX].equalsIgnoreCase("t")) {
            returnValue = "T";
        }

        return (returnValue);
    }

    public int getSlotNumber() {
        int returnVal = 0;

        try {
            returnVal = Integer.parseInt(miObjectStructure[MI_SLOTNUMBER_INDEX]);
        } catch (NumberFormatException ex) {
            returnVal = 0;
        }

        return (returnVal);
    }

    public int getActionCode() {
        int returnVal = 0;

        try {
            returnVal = Integer.parseInt(miObjectStructure[MI_ACTION_INDEX]);
        } catch (NumberFormatException ex) {
            returnVal = 0;
        }

        return (returnVal);
    }

    public String getActionReason() {
        return(miObjectStructure[MI_ACTIONREASON_INDEX]);
    }
    
    public String getTargetValue() {
        return (miObjectStructure[MI_NEWVALUE_INDEX]);
    }

}
