package ru.nw.spiiras.nv;

import java.util.List;
import java.util.Random;
import com.vaadin.addon.charts.Chart;
import com.vaadin.addon.charts.model.*;
import com.vaadin.addon.charts.model.style.SolidColor;
import com.vaadin.annotations.Theme;
import com.vaadin.annotations.VaadinServletConfiguration;
import com.vaadin.annotations.Widgetset;
import com.vaadin.server.VaadinRequest;
import com.vaadin.server.VaadinServlet;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.TextField;
import com.vaadin.ui.UI;

import javax.servlet.annotation.WebServlet;

/**
 * This UI is the application entry point. A UI may either represent a browser window 
 * (or tab) or some part of a html page where a Vaadin application is embedded.
 * <p>
 * The UI is initialized using {@link #init(VaadinRequest)}. This method is intended to be 
 * overridden to add component to the user interface and initialize non-component functionality.
 */
@Theme("mytheme")
@Widgetset("ru.nw.spiiras.nv.MyAppWidgetset")
public class MyUI extends UI {

    private Chart ipSummaryChart;
    private Chart dnsSummaryChart;
    private Chart synAckRateChart;
    private Chart httpRequestsChart;
    private TextField alerts;
    private ApplicationManager applicationManager;

    @Override
    protected void init(VaadinRequest vaadinRequest) {
        final GridLayout layout = new GridLayout(2,4);
        applicationManager = new ApplicationManager(this);
        com.vaadin.ui.Label label = new com.vaadin.ui.Label("Network Analysis Application");

        dnsSummaryChart = buildDnsSummaryChart();
        synAckRateChart = buildSynAckRateChart();
        httpRequestsChart = buildHttpRequestsChart();
        ipSummaryChart = buildIpSummaryChart();
        alerts = new TextField();
        alerts.setValue("192.168.0.14 sent malicious script");
        layout.addComponent(label, 0, 0, 1, 0);
        layout.addComponent(ipSummaryChart, 0, 1);
        layout.addComponent(dnsSummaryChart, 0, 2);
        layout.addComponent(synAckRateChart, 1, 1);
        layout.addComponent(httpRequestsChart, 1, 2);
        layout.addComponent(alerts, 0 , 3, 1, 3);
        layout.setMargin(true);
        layout.setSpacing(true);
        layout.setSizeFull();
        setContent(layout);
    }

    private Chart buildHttpRequestsChart() {
        httpRequestsChart = new Chart();
        Configuration configuration = httpRequestsChart.getConfiguration();
        configuration.setTitle("HTTP Requests Rate");
        configuration.getxAxis().setTitle("time, minutes");
        configuration.getyAxis().setTitle("request");
        configuration.getChart().setType(ChartType.SPLINE);


        httpRequestsChart.setWidth(80, Unit.PERCENTAGE);
        return httpRequestsChart;
    }

    private Chart buildSynAckRateChart() {
        synAckRateChart = new Chart();
        Configuration configuration = synAckRateChart.getConfiguration();
        configuration.setTitle("Syn Ack Rate");
        configuration.getxAxis().setTitle("time, minutes");
        configuration.getyAxis().setTitle("SYN/ACK");
        configuration.getChart().setType(ChartType.SPLINE);

        synAckRateChart.setWidth(80, Unit.PERCENTAGE);
        return synAckRateChart;
    }

    private Chart buildDnsSummaryChart() {
        dnsSummaryChart = new Chart();
        Configuration configuration = dnsSummaryChart.getConfiguration();

        configuration.setTitle("DNS requests per Minute");
        configuration.getxAxis().setTitle("time, minutes");
        configuration.getyAxis().setTitle("requests");
        configuration.getChart().setType(ChartType.SPLINE);

        dnsSummaryChart.setWidth(80, Unit.PERCENTAGE);

        return dnsSummaryChart;
    }

    private Chart buildIpSummaryChart() {
        ipSummaryChart = new Chart();
        Configuration configuration = ipSummaryChart.getConfiguration();
        configuration.setTitle("IP Packets per Minute");
        configuration.getxAxis().setTitle("time, minutes");
        configuration.getyAxis().setTitle("packets");
        configuration.getChart().setType(ChartType.SPLINE);

        ipSummaryChart.setWidth(80, Unit.PERCENTAGE);
        return ipSummaryChart;
    }

    public void onDataChanged(List<?> data) {
        for(Object o:data) {
            refreshCharts(o);
        }
    }

    private void refreshCharts(Object o) {}

    @WebServlet(urlPatterns = "/*", name = "MyUIServlet", asyncSupported = true)
    @VaadinServletConfiguration(ui = MyUI.class, productionMode = false)
    public static class MyUIServlet extends VaadinServlet {
    }
}
