//https://github.com/scalafx/ScalaFX-Tutorials/tree/master/scalafxml-example
//how to use fxml with scala

import java.io.IOException

import javafx.scene.image.Image
import scalafx.Includes._
import scalafx.application.JFXApp
import scalafx.application.JFXApp.PrimaryStage
import scalafx.scene.Scene
import scalafxml.core.{FXMLView, NoDependencyResolver}



object FXMLTwitterAnalyzerForm extends JFXApp {

  val resource = getClass.getResource("TwitterAnalyzerForm.fxml")
  if (resource == null) {
    throw new IOException("Cannot load resource: TwitterAnalyzerForm.fxml")
  }

  val root = FXMLView(resource, NoDependencyResolver)

  stage = new PrimaryStage() {
    title = "Twitter Big Data Project"
    scene = new Scene(root)
    icons.add(new Image("waterfall.jpeg"))
    maximized = false
    resizable = false
  }

}
