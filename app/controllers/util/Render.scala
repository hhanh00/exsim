package controllers.util

import play.api.mvc._
import play.api.Play
import play.api.Play.current
import org.fusesource.scalate._
import org.fusesource.scalate.util.FileResourceLoader
import org.fusesource.scalate.layout.DefaultLayoutStrategy
import play.api.http._
import org.fusesource.scalate.util._
import java.util.Scanner

object Render {
  
  val scalateEngine = {
    val engine = new TemplateEngine
    engine.packagePrefix = "webTmpl"
    engine.layoutStrategy = new DefaultLayoutStrategy(engine, "layout/default.scaml")
    engine.classpath = "tmp/classes"
    engine.workingDirectory = Play.getFile("tmp")
    engine.combinedClassPath = true
    engine.classLoader = Play.classloader
    engine.importStatements = List("import controllers.routes._",
        "import models._",
        "import play.api._")
    engine
  }
  def withStds(template: String, args: (Symbol, Any)*)(implicit request: Request[AnyContent]) =
    Template(template).render(args map { case (k, v) => k.name -> v } toMap)

  case class Template(name: String) {
    def render(attributes: Map[String, Any]) = ScalateContent {
    scalateEngine.layout(name, attributes)
    }
  }
  
  case class ScalateContent(content: String)
  
  implicit def toWriteable(implicit codec: Codec): Writeable[ScalateContent] = Writeable[ScalateContent](
    (c: ScalateContent) => c.content.getBytes(codec.charset))
    
  implicit def toContentType(implicit codec: Codec): ContentTypeOf[ScalateContent] = ContentTypeOf[ScalateContent](Some(ContentTypes.HTML))
}