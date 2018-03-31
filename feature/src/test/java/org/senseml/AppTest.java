package org.senseml;

import org.junit.Test;

import java.util.List;

/**
 * Unit test for simple App.
 */
public class AppTest {


    @Test
    public void testMain() {
        System.out.println("start test SenseML main!!!");

        // rawdata





        System.out.println("end test SenseML main!!!");
    }

    @Test
    public void testDateSet() {
        List<String> orders = DateSet.getOrdersData();
        System.out.println("orders:");
        System.out.println(orders);
    }
}
