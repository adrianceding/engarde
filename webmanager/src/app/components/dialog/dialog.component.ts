import { Component, Inject } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';

@Component({
  selector: 'custom-dialog',
  templateUrl: './dialog.component.html',
  styleUrls: ['./dialog.component.scss'],
  standalone: false
})
export class DialogComponent {
  title: string;
  content: string;
  thingsToBeDone: Array<{
    label: string,
    whatToDo: () => void
  }>;

  constructor(
    public dialogRef: MatDialogRef<DialogComponent>,
    @Inject(MAT_DIALOG_DATA) public data: any
  ) {
    dialogRef.disableClose = false;
    this.thingsToBeDone = data.thingsToBeDone || [];
    this.title = data.title;
    this.content = data.content;
  }

  doStuff(whatToDo: () => void) {
    if (whatToDo) {
      whatToDo();
    }
    this.dialogRef.close();
  }
}
