package org.apache.ignite.springdata.compoundkey;

import java.util.Objects;

public class CityKeyExt extends CityKey {

    private long idExt;

    public CityKeyExt(int id, String countryCode, long idExt) {
        super(id, countryCode);
        this.idExt = idExt;
    }

    /**
     * @return city extended id
     * */
    public long getIdExt() {
        return idExt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        CityKeyExt that = (CityKeyExt) o;
        return idExt == that.idExt;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), idExt);
    }

    @Override
    public String toString() {
        return "CityKeyExt{" +
                "idExt=" + idExt +
                "} " + super.toString();
    }
}
