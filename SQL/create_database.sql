create database users_data
GO

use users_data
create table users (
    UserID int,
    DeviceModel varchar(30),
    OperatingSystem varchar(20),
    AppUsageTimeMinPerDay int,
    ScreenOnTimeMinPerDay float,
    BatteryDrainPerDay int,
    AppsInstalledCount int,
    DataUsagePerDay int,
    UserAge int,
    UserGender char,
    BehaviorClass int,
    BehaviorLabel int,
    BatteryEfficiency int,
    primary key UserID
);