export class GridPower {

    public X : number;
    public Y : number;
    public power : number;

    constructor(X: number, Y: number, power: number) {
        this.X = X;
        this.Y = Y;
        this.power = power;
    }
}

export class GridPowers {

    public gridPowerArray : GridPower[];
}