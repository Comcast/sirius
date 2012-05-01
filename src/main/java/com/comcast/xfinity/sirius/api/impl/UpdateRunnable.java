package com.comcast.xfinity.sirius.api.impl;

class UpdateRunnable implements Runnable {

    private SiriusCommand command;
    
    UpdateRunnable(SiriusCommand command){
        this.command = command;
    }
    
    public SiriusCommand getCommand() {
        return command;
    }

    @Override
    public void run() {
        command.getHandler().handleUpdate(command.getRequest());
    }   
}