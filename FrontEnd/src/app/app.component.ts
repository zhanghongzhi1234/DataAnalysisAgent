import { Component, AfterViewInit, ViewChild, ElementRef } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { LocationModel} from './app.locationmodel';
import { GridPower, GridPowers} from './app.gridpower';
//import 'bootstrap/dist/css/bootstrap.min.css';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
  //styleUrls: ['bootstrap/dist/css/bootstrap.min.css']
})

export class AppComponent implements AfterViewInit {
  
  //locationModel : LocationModel;
  @ViewChild('canvas1') canvas1: ElementRef;
  @ViewChild('select1') solutionDropList: ElementRef;
  @ViewChild('lblDetail') lblDetail: ElementRef;
  private context: CanvasRenderingContext2D;
  private url = 'http://localhost:8080/api/wind/location';            //api is from DataAnalysisAgent
  gridNumberX = 198;          //grid number in X, original value is 198
  gridNumberY = 134;          //grid number in Y, original value is 134
  minPower = 0;
  maxPower = 500;
  private gridUnitInMeters = 50;      //Grid real size in meter
  private gridSizeInPixel = 40;       //Grid cell size in pixel
  private padding = 10;               //Grid area padding in pixel

  private radius = 20;
  private fillColor = "lime";

  private textFont = "14px Times New Roman";
  private textColor = "blue";

  private powerValues : number[];
  private girdPowers : GridPower[];
  private solutions: GridPower[][];

  postData = {
    test: 'mycontent',
  };

  constructor(private httpClient : HttpClient) {
  }

  ngAfterViewInit(): void {
    this.context = (this.canvas1.nativeElement as HTMLCanvasElement).getContext('2d');
    this.drawGrid();
  }

  private resetCanvas() {
    this.context.beginPath();
    this.context.clearRect(0, 0 , this.context.canvas.width, this.context.canvas.height);
    this.context.closePath();
    this.drawGrid();
  }

  private drawGrid() {
    var bw = this.gridNumberX * this.gridSizeInPixel;     // Box width
    var bh = this.gridNumberY * this.gridSizeInPixel;     // Box height

    this.context.canvas.width = bw + this.padding * 2;
    this.context.canvas.height = bh + this.padding * 2;

    //draw vertical line
    for (let i = 0; i <= this.gridNumberX; i++) {
      this.context.moveTo(0.5 + i * this.gridSizeInPixel + this.padding, this.padding);
      this.context.lineTo(0.5 + i * this.gridSizeInPixel + this.padding, bh + this.padding);
    }

    //draw horizon line
    for (let i = 0; i <= this.gridNumberY; i++) {
      this.context.moveTo(this.padding, 0.5 + i * this.gridSizeInPixel + this.padding);
      this.context.lineTo(bw + this.padding, 0.5 + i * this.gridSizeInPixel + this.padding);
    }
    this.context.strokeStyle = "black";
    this.context.stroke();
  }

  private drawCircle(x: number, y: number) {

    let xCenter = this.gridSizeInPixel / 2 + (x - 1) * this.gridSizeInPixel + this.padding;
    let yCenter = this.gridSizeInPixel / 2 + (y - 1) * this.gridSizeInPixel + this.padding;
    this.context.beginPath();
    this.context.arc(xCenter, yCenter, this.radius, 0, 2*Math.PI);
    this.context.closePath();
    this.context.fillStyle = this.fillColor;
    this.context.fill();
  }

  private drawText(x: number, y: number, text: string) {
    let xCenter = (x - 0.7) * this.gridSizeInPixel + this.padding;
    let yCenter = (y - 0.3) * this.gridSizeInPixel + this.padding;
    this.context.font = this.textFont;
    this.context.fillStyle = this.textColor;
    this.context.fillText(text, xCenter, yCenter);
  }

  onClickGenerateRandomPower() {
    this.resetCanvas();
    let selectElement = (this.solutionDropList.nativeElement as HTMLSelectElement);
    selectElement.options.length = 0;
    this.girdPowers = [];
    this.powerValues = [];
    for(let i = 1; i <= this.gridNumberX; i++) {
      for(let j = 1; j <= this.gridNumberY; j++) {
        let power : number = Math.round(Math.random() * (this.maxPower - this.minPower) + this.minPower);
        this.powerValues.push(power);
        this.girdPowers.push(new GridPower(i,j, power));
        this.drawText(i, j, power.toString());
      }
    }
    //this.powerArray = powers.join(",");
  }

  onClickPost(turbineType: string) {
    let locationModel : LocationModel = new LocationModel();
    locationModel.setData(turbineType, this.powerValues.join(","), this.gridNumberX, this.gridNumberY, this.gridUnitInMeters);
    //var temp = this.httpClient.get("http://127.0.0.1:8080/api/wind/test");
    //this.drawCircle(1, 5);
    this.httpClient.post(this.url, locationModel).toPromise().then((data:any) => {
      console.log(data);
      console.log(data.value);
      this.solutions = JSON.parse(data.value);
      if(this.solutions.length > 0) {
        this.resetCanvas();
        let selectElement = (this.solutionDropList.nativeElement as HTMLSelectElement);
        selectElement.options.length = 0;
        for(let i = 0; i < this.solutions.length; i++) {
          let totalPower = 0;
          let gridPowers = this.solutions[i];
          gridPowers.forEach(gridPower => {
            totalPower += gridPower.power;
            if(i == 0) {
              this.drawCircle(gridPower.X, gridPower.Y);
              //this.drawText(gridPower.X, gridPower.Y, gridPower.power.toString());      //if can move Circle to bottom layer, no need drawText again. will canvas support layer?
            }
          });
          let optionElement : HTMLOptionElement = new Option();
          optionElement.text = "方案" + (i + 1) + "，总功率" + totalPower;
          optionElement.value = i.toString();
          selectElement.options.add(optionElement);
        }
        this.drawAllPowerTexts();
        let detailElement = this.lblDetail.nativeElement as HTMLLabelElement;
        detailElement.textContent = "共" + this.solutions[0].length + "点，"+ JSON.stringify(this.solutions[0]);
      }
    })
  }

  onChangeSolution() {
    let selectElement = (this.solutionDropList.nativeElement as HTMLSelectElement);
    let detailElement = this.lblDetail.nativeElement as HTMLLabelElement;
    let selectIndex = selectElement.selectedIndex;
    let gridPowers = this.solutions[selectIndex];
    this.resetCanvas();
    gridPowers.forEach(gridPower => {
        this.drawCircle(gridPower.X, gridPower.Y);
    });
    this.drawAllPowerTexts();
    detailElement.textContent = "共" + gridPowers.length + "点，"+ JSON.stringify(gridPowers);
  }

  onClickResetGridNumber() {
    this.resetCanvas();
    let selectElement = (this.solutionDropList.nativeElement as HTMLSelectElement);
    selectElement.options.length = 0;
  }

  drawAllPowerTexts() {
    this.girdPowers.forEach(gridPower => {
      this.drawText(gridPower.X, gridPower.Y, gridPower.power.toString());
    })
  }
}
