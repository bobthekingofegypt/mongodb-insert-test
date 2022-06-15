package org.bobstuff;

import org.bobstuff.models.MainThing;
import org.bson.types.ObjectId;

public class Models {
    public static MainThing createMainThing() {
        var m = new MainThing();
        m.setSomeId(new ObjectId().toString());

        return m;
    }
}
