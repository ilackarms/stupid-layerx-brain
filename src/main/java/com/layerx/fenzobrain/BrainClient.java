package com.layerx.fenzobrain;

import com.google.gson.Gson;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.mesos.Protos;

import java.io.*;

public class BrainClient {
    private static final String BRAIN_ID="Fenzo-Brain";
    private static final String BRAIN_URL="http://127.0.0.1:3001";

    private static final String API_PREFIX="/api/v1";
    private static final String REGISTER_PATH=API_PREFIX+"/register";
    private static final String SCHEDULE_PATH=API_PREFIX+"/schedule";
    private final String layerxURL;

    public BrainClient(String layerxURL){
        this.layerxURL = layerxURL;
    }

    public void scheduleTasks(Protos.TaskInfo task, Protos.Offer offer) throws UnirestException, IOException {
        String taskID = task.getTaskId().getValue();
        String offerID = offer.getId().getValue();
        ScheduleTaskRequest scheduleTaskRequest = new ScheduleTaskRequest(taskID, offerID);
        Gson gson = new Gson();
        String requestJson = gson.toJson(scheduleTaskRequest);
        String response = httpPost(layerxURL+SCHEDULE_PATH, requestJson.getBytes());
        System.out.println("ScheduleTasks response: "+response);
    }

    public void register() throws UnirestException, IOException {
        RegisterBrainRequest registerBrainRequest = new RegisterBrainRequest(BRAIN_ID, BRAIN_URL);
        Gson gson = new Gson();
        String requestJson = gson.toJson(registerBrainRequest);
        System.out.println("Register request: "+requestJson);
        String response = httpPost(layerxURL+REGISTER_PATH, requestJson.getBytes());
        System.out.println("Register response: "+response);
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


    private class RegisterBrainRequest {
        public String brain_id;
        public String brain_url;
        public RegisterBrainRequest(String brain_id, String brain_url){
            this.brain_id = brain_id;
            this.brain_url = brain_url;
        }
    }

    private class ScheduleTaskRequest {
        public String task_id;
        public String offer_id;
        public ScheduleTaskRequest(String taskID, String offerID){
            this.task_id = taskID;
            this.offer_id = offerID;
        }
    }

    //http utils
    private static String httpPost(String urlString, byte[] requestBody) throws IOException, UnirestException {
        HttpResponse<String> stringResponse = Unirest.post(urlString)
                .body(requestBody)
                .asString();
        return stringResponse.getBody();
    }
}
