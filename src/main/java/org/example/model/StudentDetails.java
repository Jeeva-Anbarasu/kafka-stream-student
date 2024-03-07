package org.example.model;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class StudentDetails {

    public int id ;
    public String firstName;
    public String lastName;
    public ClassDetails details;
    public String location;
}
