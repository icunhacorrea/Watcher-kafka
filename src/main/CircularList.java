package main;

import java.util.ArrayList;

public class CircularList {

    private Node head;
    private Node lastConfirmed;
    private Node lastInserted;
    private int sizeMax;
    private int counter;
    ArrayList<String> received = new ArrayList<>();

    static class Node{

        private Object data;
        private Node next;
        private boolean read;
        private String key;

        Node(Object data, String key) {
            this.data = data;
            this.next = null;
            this.read = false;
            this.key = key;
        }

        public Object getData(){
            return data;
        }

        public void setData(Object o) {
            this.data = o;
        }

        public void setNext(Node next) {
            this.next = next;
        }

        public Node getNext() {
            return this.next;
        }

        public void setRead(boolean read) {
            this.read = read;
        }

        public boolean getRead() {
            return this.read;
        }

        public String getKey() {
            return this.key;
        }

        public void setKey(String key) {
            this.key = key;
        }
    }

    public CircularList(int size) {
        this.head = null;
        this.lastConfirmed = null;
        this.lastInserted = null;
        this.sizeMax = size;
        this.counter = 0;
    }

    public void insert(Object data, String key) {
        System.out.println("Entrou aqui pai. adicionando key: " + key);
        if (head == null) {
            head = new Node(data, key);
            head.setNext(head);
            incrementCounter();
            lastInserted = head;
            return;
        }

        if (getCounter() == sizeMax) {
            insertBeforeRead(data, key);
            return;
        }

        Node tmp = new Node(data, key);
        Node current = head;
        Node prev = null;

        if (current != null) {
            while (current.getNext() != head) {
                prev = current;
                current = current.getNext();
            }

            current.setNext(tmp);
            tmp.setNext(head);
        }
        incrementCounter();

    }

    public boolean insertBeforeRead(Object data, String key) {

        if (lastInserted != null) {
            if (lastInserted.getRead()) {
                // Se item já tiver sido lido.
                lastInserted.setData(data);
                lastInserted.setKey(key);
                lastInserted.setRead(false);
            }
        }

        lastInserted = lastInserted.getNext();

        return true;
    }

    public String toString(){
        String output = "";

        if (head == null)
            return "** empity **";
        if (head.getNext() == null) {
            output += "-> [" + head.getKey() + "]";
        } else {
            Node current = head;
            while(current.getNext() != head) {
                output += concatString(current);
                current = current.getNext();
            }
            output += concatString(current);
        }
        return output;
    }

    public void markRead() {
        if(lastConfirmed == null)
            lastConfirmed = head;
        if(head == null)
            return;

        synchronized (received) {
            for (String r : received) {

                // Implementar um laço que percorra de LastInserted até last
                System.out.println("Procurando por: " + r);
                System.out.println("LastInserted: " + lastInserted.getKey());


                while ()

                if (lastConfirmed.getKey().equals(r)) {
                    System.out.println("Marcando a key [" + r + "]" + " como recebida.");
                    lastConfirmed.setRead(true);
                    lastConfirmed = lastConfirmed.getNext();
                }


            }
            clearReceived();
        }
    }

    public String concatString(Node current) {
        return " -> [" + current.getKey() + " / " + current.getRead() + "]";
    }

    public void incrementCounter() {
        counter++;
    }

    public int getCounter() {
        return counter;
    }

    public boolean listIsFull() {
        return counter < sizeMax;
    }

    public void addReceived(String r) {
        synchronized (received) {
            received.add(r);
        }
    }

    public int getSizeReceived() {
        synchronized (received) {
            return received.size();
        }
    }

    public void clearReceived() {
        synchronized (received) {
            received.clear();
        }
    }

    public Node getLastInserted() {
        return lastConfirmed;
    }

    public Node getLastUnread() {
        return lastConfirmed;
    }
}
