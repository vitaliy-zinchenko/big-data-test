package zinjvi;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.OperatingSystem;
import eu.bitwalker.useragentutils.UserAgent;

/**
 * Hello world!
 */
public class App {

    public static void main(String[] args) {
        System.out.println("start");

        String s = "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)";
        UserAgent userAgent = new UserAgent(s);

        Browser browser = userAgent.getBrowser();
        System.out.println("UA type: " + browser.getBrowserType().getName());
        System.out.println("UA family: " + browser.getGroup().name());

        OperatingSystem operatingSystem = userAgent.getOperatingSystem();
        System.out.println("OS name: " + operatingSystem.getName());
        System.out.println("Device: " + operatingSystem.getDeviceType().getName());

        System.out.println("end");
    }
}

//        start
//        UA type: Browser
//        UA family: IE
//        OS name: Windows XP
//        Device: Computer
//        end
