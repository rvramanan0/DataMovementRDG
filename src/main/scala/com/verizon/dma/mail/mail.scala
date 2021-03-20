
import com.typesafe.config.ConfigFactory

import scala.beans.BeanProperty

object mail extends App {

  val conf = ConfigFactory.load("application.conf")

  implicit def stringToSeq(single: String): Seq[String] = Seq(single)
  implicit def liftToOption[T](t: T): Option[T] = Some(t)

  sealed abstract class MailType
  case object Plain extends MailType
  case object Rich extends MailType
  case object MultiPart extends MailType

  case class Mail(
                   from: (String, String), // (email -> name)
                   to: Seq[String],
                   cc: Seq[String] = Seq.empty,
                   bcc: Seq[String] = Seq.empty,
                   subject: String,
                   message: String,

                   richMessage: Option[String] = None,

                   attachment: Option[(java.io.File)] = None
                 )

  object send {


    def a(mail: Mail) {
      import org.apache.commons.mail._

      val format =
        if (mail.attachment.isDefined) MultiPart
        else if (mail.richMessage.isDefined) Rich
        else Plain

      val commonsMail: Email = format match {
        case Plain => new SimpleEmail().setMsg(mail.message)
        case Rich => new HtmlEmail().setHtmlMsg(mail.richMessage.get).setTextMsg(mail.message)
        case MultiPart => {
          val attachment = new EmailAttachment()
          attachment.setPath(mail.attachment.get.getAbsolutePath)
          attachment.setDisposition(EmailAttachment.ATTACHMENT)
          attachment.setName(mail.attachment.get.getName)
          new MultiPartEmail().attach(attachment).setMsg(mail.message)
        }
      }

      // TODO Set authentication from your configuration, sys properties or w/e

      // Can't add these via fluent API because it produces exceptions
      mail.to foreach (commonsMail.addTo(_))
      mail.cc foreach (commonsMail.addCc(_))
      mail.bcc foreach (commonsMail.addBcc(_))


      // NBCU com.verizon.dma.mail config

      commonsMail.setHostName(conf.getString("emailParams.email.server"))
      //commonsMail.setAuthentication("email","pass")
      //commonsMail.setSSLOnConnect(true)
      commonsMail.setSmtpPort(conf.getString("emailParams.email.port").toInt)


      commonsMail.
        setFrom(mail.from._1, mail.from._2).
        setSubject(mail.subject).
        send()
    }
  }

  def functionExample(message2:String)  {

    send a new Mail (
      from = "Venkat.Ramanan.r@verizon.com" -> "Verizon",
      to = Seq("Venkat.Ramanan.r@verizon.com"),
      cc = Seq("Venkat.Ramanan.r@verizon.com"),
      subject = "DMA Failure ",
      message = "Job name error message.",
      richMessage =message2  )

  }


}
