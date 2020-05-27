public class Record {

    private int deviceid;
    private int nr_odczytu;
    private String data_czas;
    private float energia;
    private float t_zewn;
    private float v_wiatr;
    private float wilg;
    private float zachm;
    private float dlug_dnia;
    private String typ_dnia;
    private String pora_roku;

    public Record(String id, String no, String date, String energy, String temp, String wind, String humidity,
                  String cloudiness, String dayLength, String dayType, String season) {
        this.deviceid = Integer.parseInt(id);
        this.nr_odczytu = Integer.parseInt(no);
        this.data_czas = date;
        this.energia = Float.parseFloat(energy.replace(",", "."));
        this.t_zewn = Float.parseFloat(temp.replace(",", "."));
        this.v_wiatr = Float.parseFloat(wind.replace(",", "."));
        this.wilg = Float.parseFloat(humidity.replace(",", "."));
        this.zachm = Float.parseFloat(cloudiness.replace(",", "."));
        this.dlug_dnia = Float.parseFloat(dayLength.replace(",", "."));
        this.typ_dnia = dayType;
        this.pora_roku = season;
    }

    public int getDeviceid() {
        return deviceid;
    }

    public void setDeviceid(int deviceid) {
        this.deviceid = deviceid;
    }

    public int getNr_odczytu() {
        return nr_odczytu;
    }

    public void setNr_odczytu(int nr_odczytu) {
        this.nr_odczytu = nr_odczytu;
    }

    public String getData_czas() {
        return data_czas;
    }

    public void setData_czas(String data_czas) {
        this.data_czas = data_czas;
    }

    public float getEnergia() {
        return energia;
    }

    public void setEnergia(float energia) {
        this.energia = energia;
    }

    public float getT_zewn() {
        return t_zewn;
    }

    public void setT_zewn(float t_zewn) {
        this.t_zewn = t_zewn;
    }

    public float getV_wiatr() {
        return v_wiatr;
    }

    public void setV_wiatr(float v_wiatr) {
        this.v_wiatr = v_wiatr;
    }

    public float getWilg() {
        return wilg;
    }

    public void setWilg(float wilg) {
        this.wilg = wilg;
    }

    public float getZachm() {
        return zachm;
    }

    public void setZachm(float zachm) {
        this.zachm = zachm;
    }

    public float getDlug_dnia() {
        return dlug_dnia;
    }

    public void setDlug_dnia(float dlug_dnia) {
        this.dlug_dnia = dlug_dnia;
    }

    public String getTyp_dnia() {
        return typ_dnia;
    }

    public void setTyp_dnia(String typ_dnia) {
        this.typ_dnia = typ_dnia;
    }

    public String getPora_roku() {
        return pora_roku;
    }

    public void setPora_roku(String pora_roku) {
        this.pora_roku = pora_roku;
    }
}
