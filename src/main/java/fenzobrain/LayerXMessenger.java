package fenzobrain;

import com.google.gson.Gson;
import org.apache.mesos.Protos;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class LayerXMessenger {

    private static final String API_PREFIX="/api/v1";
    private static final String OFFERS_PATH=API_PREFIX+"/offers";
    private static final String TASKS_PATH=API_PREFIX+"/tasks";
    private static final String SCHEDULE_PATH=API_PREFIX+"/schedule";
    private final String layerxURL;
    private static final List<String> knownOffers = new ArrayList<>();
    private static final List<String> runningTasks = new ArrayList<>();

    public LayerXMessenger(String layerxURL){
        this.layerxURL = layerxURL;
    }

    public List<Protos.Offer> getNewResourceOffers() throws IOException {
        List<Protos.Offer> offers = new ArrayList<>();
        try {
            String offersJson = httpGet(layerxURL + OFFERS_PATH);
            Gson gson = new Gson();
            OfferJsonWrapper[] offerArray = gson.fromJson(offersJson, OfferJsonWrapper[].class);
            for (OfferJsonWrapper offerJsonWrapper : offerArray){
                byte[] data = offerJsonWrapper.protoData;
                Protos.Offer offer = Protos.Offer.parseFrom(data);
                String offerID = offer.getId().getValue();
                System.out.println("Received offer: " + offerID);
                if (!knownOffers.contains(offerID)){
                    System.out.println("Adding offer: " + offerID);
                    offers.add(offer);
                    knownOffers.add(offerID);
                } else {
                    System.out.println("Already knew about offer: "+offerID);
                }
            }
        } catch (IOException e){
            System.out.println("Failed to get Resource Offers!");
            throw e;
        }
        return offers;
    }

    public List<Protos.TaskInfo> getTasks() throws IOException {
        List<Protos.TaskInfo> tasks = new ArrayList<>();
        try {
            String tasksJson = httpGet(layerxURL + TASKS_PATH);
            Gson gson = new Gson();
            TaskJsonWrapper[] taskArray = gson.fromJson(tasksJson, TaskJsonWrapper[].class);
            for (TaskJsonWrapper taskJsonWrapper : taskArray){
                byte[] data = taskJsonWrapper.protoData;
                Protos.TaskInfo taskInfo = Protos.TaskInfo.parseFrom(data);
                String taskId = taskInfo.getTaskId().getValue();
                System.out.println("Received offer: " + taskId);
                if (!runningTasks.contains(taskId)){
                    System.out.println("Adding offer: "+taskId);
                    tasks.add(taskInfo);
                    runningTasks.add(taskId);
                } else {
                    System.out.println("Already knew about task: "+taskId);
                }
            }
        } catch (IOException e){
            System.out.println("Failed to get Resource Offers!");
            throw e;
        }
        return tasks;
    }

    public void scheduleTasks(Protos.TaskInfo task, Protos.Offer offer) throws Exception{
        String taskID = task.getTaskId().getValue();
        String offerID = offer.getId().getValue();
        ScheduleTaskRequest scheduleTaskRequest = new ScheduleTaskRequest(taskID, offerID);
        Gson gson = new Gson();
        String requestJson = gson.toJson(scheduleTaskRequest);
        String response = httpPost(layerxURL, requestJson);
        System.out.println("ScheduleTasks response: "+response);
    }

    public void declineOffer(Protos.Offer offer) throws Exception {
//        String taskID = task.getTaskId().getValue();
//        String offerID = offer.getId().getValue();
//        ScheduleTaskRequest scheduleTaskRequest = new ScheduleTaskRequest(taskID, offerID);
//        Gson gson = new Gson();
//        String requestJson = gson.toJson(scheduleTaskRequest);
//        String response = httpPost(layerxURL, requestJson);
//        System.out.println("ScheduleTasks response: "+response);
    }

    //http utils
    private static String httpGet(String urlString) throws IOException{
        HttpURLConnection connection = null;

        URL url = new URL(urlString);
        connection = (HttpURLConnection)url.openConnection();
        connection.setRequestMethod("GET");
        int responseCode = connection.getResponseCode();
        System.out.println("\nSending 'GET' request to URL : " + url);
        System.out.println("Response Code : " + responseCode);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(connection.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        //print result
        System.out.println(response.toString());
        return response.toString();
    }

    private static String httpPost(String urlString, String body) throws Exception {
        URL url = new URL(urlString);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();

        //add reuqest header
        con.setRequestMethod("POST");

        // Send post request
        con.setDoOutput(true);
        DataOutputStream wr = new DataOutputStream(con.getOutputStream());
        wr.writeBytes(body);
        wr.flush();
        wr.close();

        int responseCode = con.getResponseCode();
        System.out.println("\nSending 'POST' request to URL : " + url);
        System.out.println("Post parameters : " + body);
        System.out.println("Response Code : " + responseCode);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        //print result
        System.out.println(response.toString());
        return response.toString();
    }

    //json struct that contains a proto.offer
    private class OfferJsonWrapper {
        public byte[] protoData;
    }

    //json struct that contains a proto.taskinfo
    private class TaskJsonWrapper {
        public byte[] protoData;
    }

    private class ScheduleTaskRequest {
        public String task_id;
        public String offer_id;
        public ScheduleTaskRequest(String taskID, String offerID){
            this.task_id = taskID;
            this.offer_id = offerID;
        }
    }
}
