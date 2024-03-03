package com.wolffy.spark.structured.streming.project.mock.utils;

import javax.ws.rs.client.AsyncInvoker;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomOptions<T> {

    public int totalWeight;
    public List<T> options = new ArrayList<>();


    public static <T> RandomOptions<T> apply(T[] opts, int[] weights) {
        if (opts.length != weights.length) {
            throw new IllegalArgumentException("Opts and weights arrays must have the same length");
        }

        RandomOptions<T> randomOptions = new RandomOptions<>();
        for (int i = 0; i < weights.length; i++) {
            //总比重
            randomOptions.totalWeight += weights[i];

            for (int j = 0; j < weights[i]; j++) {
                randomOptions.options.add(opts[i]);
            }
        }
        return randomOptions;
    }

    /**
     * 获取随机的 Option 的值
     */
    public T getRandomOption() {

        Random random = new Random();
        int index = random.nextInt(totalWeight);
        return options.get(index);
    }

    public static void main(String[] args) {
        String[] names = {"张三", "李四", "ww"};
        int[] weights = {10, 30, 20};

        RandomOptions<String> opts = RandomOptions.apply(names, weights);

        System.out.println(opts.getRandomOption());
        System.out.println(opts.getRandomOption());
        System.out.println(opts.getRandomOption());
        System.out.println(opts.getRandomOption());
    }
}

