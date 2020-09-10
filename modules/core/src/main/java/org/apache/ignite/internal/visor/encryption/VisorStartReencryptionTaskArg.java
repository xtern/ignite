package org.apache.ignite.internal.visor.encryption;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

public class VisorStartReencryptionTaskArg extends VisorDataTransferObject {
    private int grpId;

    private boolean resetStatus;

    @Override protected void writeExternalData(ObjectOutput out) throws IOException {

    }

    @Override
    protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {

    }
}
