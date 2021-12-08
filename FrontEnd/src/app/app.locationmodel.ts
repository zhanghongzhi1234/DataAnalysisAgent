export class LocationModel {

    public powerArray : string;
    public turbineNumber : number;
    public spaceXInMeters : number;
    public spaceYInMeters : number;
    public gridX : number;
    public gridY : number;
    public gridUnit : number;

    public setData(turbineType : string, strPowerArray: string, gridX: number, gridY: number, gridUnit: number) {
        this.powerArray = strPowerArray;
        this.gridX = gridX;
        this.gridY = gridY;
        this.gridUnit = gridUnit;
        switch(turbineType){
            case "2MW":
                this.turbineNumber = 25;        //Total 25 wind turbine for 2MW
                this.spaceXInMeters = 146*5;    //D=146m, space in X = 5D
                this.spaceYInMeters = 146*3;    //D=146m, Space in Y = 3D
              break;
            case "3MW":
                this.turbineNumber = 16;        //Total 25 wind turbine for 2MW
                this.spaceXInMeters = 156*5;    //D=146m, space in X = 5D
                this.spaceYInMeters = 156*3;    //D=146m, Space in Y = 3D
              break;
          }
    }
   
}