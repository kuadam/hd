public class Device {

    private int deviceId;
    private String code;
    private String name;
    private String type;
    private String street;
    private String location;

    public Device(String id, String code, String name, String type, String street, String location) {
        this.deviceId = Integer.parseInt(id);
        this.code = code;
        this.name = name;
        this.type = type;
        this.street = street;
        this.location = location;
    }

    public int getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(int deviceId) {
        this.deviceId = deviceId;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }
}
